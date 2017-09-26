/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.rdma

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.ibm.disni.rdma.verbs.IbvPd
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockManagerRemoved}
import org.apache.spark.shuffle.{BaseShuffleHandle, _}
import org.apache.spark.shuffle.rdma.writer.chunkedpartitionagg.RdmaChunkedPartitionAggShuffleWriter
import org.apache.spark.shuffle.rdma.writer.wrapper.RdmaWrapperShuffleWriter
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, SortShuffleManager}
import org.apache.spark.storage.BlockManagerId

private[spark] class RdmaShuffleManager(val conf: SparkConf, isDriver: Boolean)
    extends ShuffleManager with Logging {
  private val logger = LoggerFactory.getLogger(classOf[RdmaShuffleManager])
  val rdmaShuffleConf = new RdmaShuffleConf(conf)
  override val shuffleBlockResolver = new RdmaShuffleBlockResolver(this)
  private val rdmaShuffleManagersMap = new ConcurrentHashMap[RdmaShuffleManagerId, RdmaChannel]()

  private var localRdmaShuffleManagerId: Option[RdmaShuffleManagerId] = None
  private var rdmaNode: Option[RdmaNode] = None

  // TODO: Keep RdmaPartitionLocations in serialized form in the driver to save cpu and memory
  // TODO: naming is too confusing for this type
  case class PartitionLocation(var locations: ArrayBuffer[RdmaPartitionLocation],
    promise: Option[Promise[Seq[RdmaPartitionLocation]]])
  private val partitionLocationsMap =
    new ConcurrentHashMap[Int, ConcurrentHashMap[Int, PartitionLocation]]

  val rdmaShuffleReaderStats: RdmaShuffleReaderStats = {
    if (rdmaShuffleConf.collectShuffleReaderStats) {
      new RdmaShuffleReaderStats(rdmaShuffleConf)
    } else {
      null
    }
  }

  val receiveListener = new RdmaCompletionListener {
    override def onSuccess(buf: ByteBuffer): Unit = {
      RdmaRpcMsg(buf) match {
        case publishMsg: RdmaPublishPartitionLocationsRpcMsg =>
          for (r <- publishMsg.rdmaPartitionLocations) {
            partitionLocationsMap.get(publishMsg.shuffleId).get(r.partitionId).
              synchronized {
                if (rdmaShuffleConf.shuffleWriterMethod ==
                  ShuffleWriterMethod.ChunkedPartitionAgg) {
                  // TODO: can be improved with a set or timestamps
                  var isExist = false
                  partitionLocationsMap.get(publishMsg.shuffleId).get(r.partitionId).locations.
                    foreach {
                      x: RdmaPartitionLocation =>
                        if (x.rdmaShuffleManagerId == r.rdmaShuffleManagerId &&
                          x.rdmaBlockLocation.address == r.rdmaBlockLocation.address) {
                          if (x.rdmaBlockLocation.length < r.rdmaBlockLocation.length) {
                            x.rdmaBlockLocation = r.rdmaBlockLocation
                          }
                          isExist = true
                        }
                    }
                    if (!isExist) {
                      partitionLocationsMap.get(publishMsg.shuffleId).get(r.partitionId).
                        locations += r
                    }
                } else {
                  partitionLocationsMap.get(publishMsg.shuffleId).get(r.partitionId).locations += r
                }
              }
          }

          if (!isDriver && publishMsg.isLast) {
            // Kick-off promise for executors
            val partitionLocation = partitionLocationsMap.get(
              publishMsg.shuffleId).get(publishMsg.partitionId)
            partitionLocation.promise match {
              case promise: Some[Promise[Seq[RdmaPartitionLocation]]] =>
                promise.get.trySuccess(partitionLocation.locations)
              case _ =>
            }
          }

        case fetchMsg: RdmaFetchPartitionLocationsRpcMsg =>
          assume(isDriver)
          try {
            publishPartitionLocations(
              fetchMsg.host,
              fetchMsg.port,
              fetchMsg.shuffleId,
              fetchMsg.partitionId,
              partitionLocationsMap.get(fetchMsg.shuffleId).get(fetchMsg.partitionId).locations)
          } catch {
            case e: Exception => logError("Failed to send RdmaPublishPartitionLocationsRpcMsg " + e)
          }

        case helloMsg: RdmaShuffleManagerHelloRpcMsg =>
          assume(isDriver)
          val rdmaShuffleManagerId = helloMsg.rdmaShuffleManagerId

          if (rdmaShuffleManagersMap.get(rdmaShuffleManagerId) == null) {
            val f = Future {
              getRdmaChannel(rdmaShuffleManagerId.host, rdmaShuffleManagerId.port, false)
            }
            f onSuccess {
              case rdmaChannel =>
                rdmaShuffleManagersMap.put(rdmaShuffleManagerId, rdmaChannel)
                val buffers = new RdmaAnnounceRdmaShuffleManagersRpcMsg(
                  rdmaShuffleManagersMap.keys.asScala.toSeq).toRdmaByteBufferManagedBuffers(
                    getRdmaByteBufferManagedBuffer, rdmaShuffleConf.recvWrSize)

                for ((dstRdmaShuffleManagerId, dstRdmaChannel) <- rdmaShuffleManagersMap.asScala) {
                  buffers.foreach(_.retain())

                  val listener = new RdmaCompletionListener {
                    override def onSuccess(buf: ByteBuffer): Unit = buffers.foreach(_.release())
                    override def onFailure(e: Throwable): Unit = {
                      buffers.foreach(_.release())
                      logError("Failed to send RdmaAnnounceExecutorsRpcMsg to executor: " +
                        dstRdmaShuffleManagerId + ", Exception: " + e)
                    }
                  }

                  try {
                    dstRdmaChannel.rdmaSendInQueue(
                      listener,
                      buffers.map(_.getAddress),
                      buffers.map(_.getLkey),
                      buffers.map(_.getLength.toInt))
                  } catch {
                    case e: Exception => listener.onFailure(e)
                  }
                }
                // Release the reference taken by the allocation
                buffers.foreach(_.release())
            }
          }

        case announceMsg: RdmaAnnounceRdmaShuffleManagersRpcMsg =>
          assume(!isDriver)
          for (rdmaShuffleManagerId <- announceMsg.rdmaShuffleManagerIds) {
            if (rdmaShuffleManagerId != localRdmaShuffleManagerId.get) {
              Future { getRdmaChannel(rdmaShuffleManagerId.host, rdmaShuffleManagerId.port, false) }
            }
          }

        case _ => logger.warn("RdmaCompletionListener for receive encountered an unidentified RPC")
      }
    }

    override def onFailure(e: Throwable): Unit = {
      logger.error("Exception in RdmaCompletionListener for receive (ignoring): " + e)
    }
  }

  if (isDriver) {
    rdmaNode = Some(new RdmaNode(conf.get("spark.driver.host"), false, rdmaShuffleConf,
      receiveListener))
    rdmaShuffleConf.setDriverPort(rdmaNode.get.getLocalInetSocketAddress.getPort.toString)
  }

  // Called on the driver only!
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    assume(isDriver)
    if (localRdmaShuffleManagerId.isEmpty) {
      localRdmaShuffleManagerId = Some(new RdmaShuffleManagerId(
        rdmaNode.get.getLocalInetSocketAddress.getHostString,
        rdmaNode.get.getLocalInetSocketAddress.getPort,
        SparkEnv.get.blockManager.blockManagerId))
    }

    SparkContext.getOrCreate(conf).addSparkListener(
      new SparkListener {
        override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
          synchronized {
            // Remove all of the RdmaPartitionLocations with this BlockManagerId
            partitionLocationsMap.values.asScala.foreach(_.values.asScala.foreach {
              partition => partition.synchronized {
                partition.locations = partition.locations.filter(
                  _.rdmaShuffleManagerId.blockManagerId != blockManagerRemoved.blockManagerId)
              }
            })

            // Remove this BlockManagerId from the list
            rdmaShuffleManagersMap.keys.asScala.foreach { rdmaShuffleManagerId =>
              if (rdmaShuffleManagerId.blockManagerId == blockManagerRemoved.blockManagerId) {
                rdmaShuffleManagersMap.remove(rdmaShuffleManagerId)
                logger.info("BlockManager " + blockManagerRemoved.blockManagerId + " is removed," +
                  " removing associated rdmaChannel from rdmaShuffleManagersMap")
              }
            }
          }
        }
      })

    val partitionHashMap = new ConcurrentHashMap[Int, PartitionLocation](
      dependency.partitioner.numPartitions)
    for (partId <- 0 until dependency.partitioner.numPartitions) {
      partitionHashMap.put(partId, PartitionLocation(new ArrayBuffer[RdmaPartitionLocation], None))
    }
    partitionLocationsMap.put(shuffleId, partitionHashMap)

    // BypassMergeSortShuffleWriter is not supported since it is package private
    if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  private def startRdmaNodeIfMissing(): Unit = {
    assume(!isDriver)
    var shouldSendHelloMsg = false
    synchronized {
      if (localRdmaShuffleManagerId.isEmpty) {
        require(rdmaNode.isEmpty)
        shouldSendHelloMsg = true
        rdmaNode = Some(new RdmaNode(SparkEnv.get.blockManager.blockManagerId.host, !isDriver,
          rdmaShuffleConf, receiveListener))
        localRdmaShuffleManagerId = Some(new RdmaShuffleManagerId(
          rdmaNode.get.getLocalInetSocketAddress.getHostString,
          rdmaNode.get.getLocalInetSocketAddress.getPort,
          SparkEnv.get.blockManager.blockManagerId))
      }
    }

    require(rdmaNode.isDefined)
    // Establish a connection to the driver in the background
    if (shouldSendHelloMsg) {
      val f = Future {
        getRdmaChannel(rdmaShuffleConf.driverHost, rdmaShuffleConf.driverPort, false)
      }
      f onSuccess {
        case rdmaChannel =>
          val buffers = new RdmaShuffleManagerHelloRpcMsg(localRdmaShuffleManagerId.get).
            toRdmaByteBufferManagedBuffers(getRdmaByteBufferManagedBuffer,
              rdmaShuffleConf.recvWrSize)

          val listener = new RdmaCompletionListener {
            override def onSuccess(buf: ByteBuffer): Unit = buffers.foreach(_.release())

            override def onFailure(e: Throwable): Unit = {
              buffers.foreach(_.release())
              logError("Failed to send RdmaExecutorHelloRpcMsg to driver " + e)
            }
          }

          try {
            rdmaChannel.rdmaSendInQueue(
              listener,
              buffers.map(_.getAddress),
              buffers.map(_.getLkey),
              buffers.map(_.getLength.toInt))
          } catch {
            case e: Exception => listener.onFailure(e)
          }
      }
    }
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    // RdmaNode can't be initialized in the c'tor for executors, so the first call will initialize
    startRdmaNodeIfMissing()

    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    // registerShuffle() is only called on the driver, so we let the first caller of getReader() to
    // initialize the structures for a new ShuffleId, in case getWriter wasn't called earlier
    partitionLocationsMap.putIfAbsent(baseShuffleHandle.shuffleId,
      new ConcurrentHashMap[Int, PartitionLocation]())

    new RdmaShuffleReader(baseShuffleHandle, startPartition, endPartition, context)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    // RdmaNode can't be initialized in the c'tor for executors, so the first call will initialize
    startRdmaNodeIfMissing()

    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    // registerShuffle() is only called on the driver, so we let the first caller of getWriter() to
    // initialize the structures for a new ShuffleId
    shuffleBlockResolver.newShuffleWriter(baseShuffleHandle)
    partitionLocationsMap.putIfAbsent(baseShuffleHandle.shuffleId,
      new ConcurrentHashMap[Int, PartitionLocation]())

    rdmaShuffleConf.shuffleWriterMethod match {
      case ShuffleWriterMethod.Wrapper =>
        new RdmaWrapperShuffleWriter(shuffleBlockResolver, baseShuffleHandle, mapId, context)
      case ShuffleWriterMethod.ChunkedPartitionAgg =>
        new RdmaChunkedPartitionAggShuffleWriter(shuffleBlockResolver, baseShuffleHandle,
          mapId, context)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleBlockResolver.removeShuffle(shuffleId)
    partitionLocationsMap.remove(shuffleId)
    true
  }

  override def stop(): Unit = {
    if (rdmaShuffleReaderStats != null) {
      rdmaShuffleReaderStats.printRemoteFetchHistogram()
    }
    shuffleBlockResolver.stop()
    rdmaNode match {
      case Some(x) => x.stop()
      case _ =>
    }
  }

  def publishPartitionLocations(host : String, port : Int, shuffleId: Int, partitionId : Int,
      rdmaPartitionLocations: Seq[RdmaPartitionLocation]) {
    // TODO: Avoid blocking by defining a future with onsuccess that will perform the send
    val rdmaChannel = getRdmaChannel(host, port, true)

    val buffers = new RdmaPublishPartitionLocationsRpcMsg(
      shuffleId,
      partitionId,
      rdmaPartitionLocations).
      toRdmaByteBufferManagedBuffers(getRdmaByteBufferManagedBuffer, rdmaShuffleConf.recvWrSize)

    val listener = new RdmaCompletionListener {
      override def onSuccess(buf: ByteBuffer): Unit = buffers.foreach(_.release())
      override def onFailure(e: Throwable): Unit = {
        buffers.foreach(_.release())
        logError("Failed to send RdmaPublishPartitionLocationsRpcMsg to " + host + ":" + port +
          ", Exception: " + e)
      }
    }

    try {
      rdmaChannel.rdmaSendInQueue(
        listener,
        buffers.map(_.getAddress),
        buffers.map(_.getLkey),
        buffers.map(_.getLength.toInt))
    } catch {
      case e: Exception =>
        listener.onFailure(e)
        throw e
    }
  }

  def fetchRemotePartitionLocations(shuffleId: Int, partitionId : Int)
      : Future[Seq[RdmaPartitionLocation]] = {
    assume(!isDriver)
    // TODO: Avoid blocking by defining a future with onsuccess that will perform the send
    val rdmaChannel = getRdmaChannel(rdmaShuffleConf.driverHost, rdmaShuffleConf.driverPort, true)

    val fetchRemotePartitionLocationPromise: Promise[Seq[RdmaPartitionLocation]] = Promise()
    // We assume that only one consumer mutates partitionLocationsMap for this particular
    // (shuffleId, partitionId)
    require (partitionLocationsMap.get(shuffleId) != null)
    partitionLocationsMap.get(shuffleId).put(partitionId, PartitionLocation(
      new ArrayBuffer[RdmaPartitionLocation], Some(fetchRemotePartitionLocationPromise)))

    val buffers = new RdmaFetchPartitionLocationsRpcMsg(localRdmaShuffleManagerId.get.host,
      localRdmaShuffleManagerId.get.port, shuffleId, partitionId).toRdmaByteBufferManagedBuffers(
        getRdmaByteBufferManagedBuffer, rdmaShuffleConf.recvWrSize)

    val listener = new RdmaCompletionListener {
      override def onSuccess(buf: ByteBuffer): Unit = buffers.foreach(_.release())
      override def onFailure(e: Throwable): Unit = {
        buffers.foreach(_.release())
        logError("Failed to send RdmaFetchPartitionLocationsRpcMsg to driver " + e)
      }
    }

    try {
      rdmaChannel.rdmaSendInQueue(
        listener,
        buffers.map(_.getAddress),
        buffers.map(_.getLkey),
        buffers.map(_.getLength.toInt))
    } catch {
      case e: Exception =>
        listener.onFailure(e)
        throw e
    }

    fetchRemotePartitionLocationPromise.future
  }

  def getRdmaChannel(host: String, port: Int, mustRetry: Boolean): RdmaChannel =
    rdmaNode.get.getRdmaChannel(new InetSocketAddress(host, port), mustRetry)

  def getRdmaByteBufferManagedBuffer(length : Int): RdmaByteBufferManagedBuffer = {
    new RdmaByteBufferManagedBuffer(new RdmaRegisteredBuffer(rdmaNode.get.getRdmaBufferManager,
      length), length)
  }

  def getRdmaRegisteredBuffer(length : Int): RdmaRegisteredBuffer = {
    new RdmaRegisteredBuffer(rdmaNode.get.getRdmaBufferManager, length)
  }

  // TODO: Clean this disni dependency out?
  def getIbvPd: IbvPd = rdmaNode.get.getRdmaBufferManager.getPd

  def getLocalRdmaShuffleManagerId: RdmaShuffleManagerId = localRdmaShuffleManagerId.get
}
