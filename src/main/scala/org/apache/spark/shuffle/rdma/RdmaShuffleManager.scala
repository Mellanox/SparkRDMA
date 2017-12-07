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
import java.util.concurrent.atomic.AtomicInteger

import com.ibm.disni.rdma.verbs.IbvPd
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockManagerRemoved}
import org.apache.spark.shuffle.{BaseShuffleHandle, _}
import org.apache.spark.shuffle.rdma.writer.wrapper.RdmaWrapperShuffleWriter
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, SortShuffleManager}
import org.apache.spark.storage.BlockManagerId

private[spark] class RdmaShuffleManager(val conf: SparkConf, isDriver: Boolean)
    extends ShuffleManager with Logging {
  val rdmaShuffleConf = new RdmaShuffleConf(conf)
  override val shuffleBlockResolver = new RdmaShuffleBlockResolver(this)
  private var localRdmaShuffleManagerId: Option[RdmaShuffleManagerId] = None
  private var rdmaNode: Option[RdmaNode] = None

  // Used by driver only
  private val mapTaskOutputsByBlockManagerId = new ConcurrentHashMap[BlockManagerId,
    scala.collection.concurrent.Map[Int, scala.collection.concurrent.Map[Int,
      RdmaMapTaskOutput]]]().asScala
  private val rdmaShuffleManagersMap =
    new ConcurrentHashMap[RdmaShuffleManagerId, RdmaChannel]().asScala
  private val blockManagerIdToRdmaShuffleManagerId =
    new ConcurrentHashMap[BlockManagerId, RdmaShuffleManagerId]().asScala

  // Used by executor only
  private val fetchMapStatusCallbackMap =
    new ConcurrentHashMap[Integer, (Int, RdmaShuffleManagerId, Seq[RdmaBlockLocation]) => Unit]
  private val fetchMapStatusCallbackIndex = new AtomicInteger(0)
  val rdmaShuffleReaderStats: RdmaShuffleReaderStats = {
    if (rdmaShuffleConf.collectShuffleReaderStats) {
      new RdmaShuffleReaderStats(rdmaShuffleConf)
    } else {
      null
    }
  }

  // Shared implementation for receive RPC handling for both driver and executors
  val receiveListener = new RdmaCompletionListener {
    override def onSuccess(buf: ByteBuffer): Unit = {
      RdmaRpcMsg(buf) match {
        case helloMsg: RdmaShuffleManagerHelloRpcMsg =>
          // Each executor advertises itself to the driver, so the driver can announce all
          // executor RDMA addresses to all other executors. This is used for establishing RDMA
          // connections in the background, so connections will be ready when shuffle phases start
          assume(isDriver)
          if (!rdmaShuffleManagersMap.contains(helloMsg.rdmaShuffleManagerId)) {
            // Book keep mapping from BlockManagerId to RdmaShuffleManagerId
            blockManagerIdToRdmaShuffleManagerId.put(helloMsg.rdmaShuffleManagerId.blockManagerId,
              helloMsg.rdmaShuffleManagerId)
            Future {
              getRdmaChannel(helloMsg.rdmaShuffleManagerId, mustRetry = false)
            }.onSuccess { case rdmaChannel =>
              rdmaShuffleManagersMap.put(helloMsg.rdmaShuffleManagerId, rdmaChannel)
              val buffers = new RdmaAnnounceRdmaShuffleManagersRpcMsg(
                rdmaShuffleManagersMap.keys.toSeq).toRdmaByteBufferManagedBuffers(
                  getRdmaByteBufferManagedBuffer, rdmaShuffleConf.recvWrSize)

              for ((dstRdmaShuffleManagerId, dstRdmaChannel) <- rdmaShuffleManagersMap) {
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
                  dstRdmaChannel.rdmaSendInQueue(listener, buffers.map(_.getAddress),
                    buffers.map(_.getLkey), buffers.map(_.getLength.toInt))
                } catch {
                  case e: Exception => listener.onFailure(e)
                }
              }
              // Release the reference taken by the allocation
              buffers.foreach(_.release())
            }
          }

        case announceMsg: RdmaAnnounceRdmaShuffleManagersRpcMsg =>
          // Driver advertises a list of known executor RDMA addresses so connection establishment
          // can be done in the background, before shuffle phases begin
          assume(!isDriver)
          announceMsg.rdmaShuffleManagerIds.filter(_ != localRdmaShuffleManagerId.get).foreach {
            rdmaShuffleManagerId => Future { getRdmaChannel(rdmaShuffleManagerId,
              mustRetry = false) }
          }

        case publishMsg: RdmaPublishMapTaskOutputRpcMsg =>
          // Executors use this message to publish their RDMA MapTaskOutputs to the driver, so that
          // the driver can later provide the information to executors on-demand
          assume(isDriver)
          val shuffleIdsMap = mapTaskOutputsByBlockManagerId.getOrElse(publishMsg.blockManagerId, {
            val newShuffleIdMap = new ConcurrentHashMap[Int,
              scala.collection.concurrent.Map[Int, RdmaMapTaskOutput]]().asScala
            mapTaskOutputsByBlockManagerId.putIfAbsent(publishMsg.blockManagerId,
              newShuffleIdMap).getOrElse(newShuffleIdMap)
          })
          val mapIdsMap = shuffleIdsMap.getOrElse(publishMsg.shuffleId, {
            val newMapIdsMap = new ConcurrentHashMap[Int, RdmaMapTaskOutput]().asScala
            shuffleIdsMap.putIfAbsent(publishMsg.shuffleId, newMapIdsMap).getOrElse(newMapIdsMap)
          })
          val rdmaMapTaskOutput = mapIdsMap.getOrElse(publishMsg.mapId, {
            val newRdmaMapTaskOutput = new RdmaMapTaskOutput(0, publishMsg.totalNumPartitions - 1)
            mapIdsMap.putIfAbsent(publishMsg.mapId, newRdmaMapTaskOutput).getOrElse(
              newRdmaMapTaskOutput)
          })
          rdmaMapTaskOutput.putRange(publishMsg.firstReduceId, publishMsg.lastReduceId,
            publishMsg.rdmaMapTaskOutput.getByteBuffer(publishMsg.firstReduceId,
              publishMsg.lastReduceId))

        case fetchMapStatusMsg: RdmaFetchMapStatusRpcMsg =>
          // Used by executors to ask the driver to translate MapStatus (shuffle block locations)
          // into their RDMA block locations, so that RDMA_READ can be used to read the data from
          // mappers
          assume(isDriver)

          // Locate MapTaskOutputs for the requested BlockManagerId and ShuffleId
          val mapIdsToMapTaskOutputMap: Option[concurrent.Map[Int, RdmaMapTaskOutput]] = try {
            Some(mapTaskOutputsByBlockManagerId(fetchMapStatusMsg.requestedBlockManagerId)(
              fetchMapStatusMsg.shuffleId))
          } catch {
            case _: NoSuchElementException =>
              logError("Unable to locate metadata for " +
                fetchMapStatusMsg.requestedBlockManagerId + " ShuffleId: " +
                fetchMapStatusMsg.shuffleId)
              None
          }

          // Extract the futures for the requested MapIds, so that we can wait for them to complete
          // once their MapTaskOutputs are ready
          val futures: Seq[Future[Unit]] = mapIdsToMapTaskOutputMap match {
            case Some(mapIds) =>
              try {
                fetchMapStatusMsg.mapIdReduceIds.map { case (mapId, _) => mapIds(mapId).fillFuture }
              } catch {
                case _: NoSuchElementException =>
                  logError("Unable to locate metadata for " +
                    fetchMapStatusMsg.requestedBlockManagerId + " ShuffleId: " +
                    fetchMapStatusMsg.shuffleId)
                  Seq.empty
              }
            case None =>
              Seq.empty
          }

          // Once the futures complete, collect the RdmaBlockLocations and respond back
          Future.sequence(futures).onSuccess { case _ =>
            blockManagerIdToRdmaShuffleManagerId.get(
                fetchMapStatusMsg.requestedBlockManagerId) match {
              case Some(requestedRdmaShuffleManagerId) =>
                val rdmaBlockLocations = fetchMapStatusMsg.mapIdReduceIds.map {
                  case (mapId, reduceId) =>
                    mapIdsToMapTaskOutputMap.get(mapId).getRdmaBlockLocation(reduceId)
                }

                // TODO: Modify response to use 16 bytes segments instead of objects
                val buffers = new RdmaFetchMapStatusResponseRpcMsg(fetchMapStatusMsg.callbackId,
                  requestedRdmaShuffleManagerId, rdmaBlockLocations).toRdmaByteBufferManagedBuffers(
                    getRdmaByteBufferManagedBuffer, rdmaShuffleConf.recvWrSize)

                val listener = new RdmaCompletionListener {
                  override def onSuccess(buf: ByteBuffer): Unit = buffers.foreach(_.release())
                  override def onFailure(e: Throwable): Unit = {
                    buffers.foreach(_.release())
                    logError("Failed to send RdmaFetchMapStatusResponseRpcMsg to " +
                      fetchMapStatusMsg.requestingRdmaShuffleManagerId + ", Exception: " + e)
                  }
                }

                val rdmaChannel = getRdmaChannel(fetchMapStatusMsg.requestingRdmaShuffleManagerId,
                  mustRetry = true)
                try {
                  rdmaChannel.rdmaSendInQueue(listener, buffers.map(_.getAddress),
                    buffers.map(_.getLkey), buffers.map(_.getLength.toInt))
                } catch {
                  case e: Exception =>
                    listener.onFailure(e)
                    throw e
                }

              case None => logError("Failed to find the RdmaShuffleManagerId for: " +
                fetchMapStatusMsg.requestedBlockManagerId + ", failing silently")
            }
          }

        case fetchMapStatusResponseMsg: RdmaFetchMapStatusResponseRpcMsg =>
          // Used by the driver to respond to RdmaFetchMapStatusRpcMsg sent by executors
          assume(!isDriver)
          fetchMapStatusCallbackMap.get(fetchMapStatusResponseMsg.callbackId).apply(
            fetchMapStatusResponseMsg.callbackId,
            fetchMapStatusResponseMsg.requestedRdmaShuffleManagerId,
            fetchMapStatusResponseMsg.rdmaBlockLocations)

        case _ => logWarning("Receive RdmaCompletionListener encountered an unidentified RPC")
      }
    }

    override def onFailure(e: Throwable): Unit = {
      logError("Exception in Receive RdmaCompletionListener (ignoring): " + e)
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
      localRdmaShuffleManagerId = Some(RdmaShuffleManagerId(
        rdmaNode.get.getLocalInetSocketAddress.getHostString,
        rdmaNode.get.getLocalInetSocketAddress.getPort,
        SparkEnv.get.blockManager.blockManagerId))

      SparkContext.getOrCreate(conf).addSparkListener(
        new SparkListener {
          override def onBlockManagerRemoved(
              blockManagerRemoved: SparkListenerBlockManagerRemoved) {
            // Remove this BlockManagerId from blockManagerIdToRdmaShuffleManagerId,
            // rdmaShuffleManagersMap and mapTaskOutputsByBlockManagerId
            blockManagerIdToRdmaShuffleManagerId.remove(
              blockManagerRemoved.blockManagerId).foreach(rdmaShuffleManagersMap.remove)
            mapTaskOutputsByBlockManagerId.remove(blockManagerRemoved.blockManagerId)
          }
        })
    }

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
        localRdmaShuffleManagerId = Some(RdmaShuffleManagerId(
          rdmaNode.get.getLocalInetSocketAddress.getHostString,
          rdmaNode.get.getLocalInetSocketAddress.getPort,
          SparkEnv.get.blockManager.blockManagerId))
      }
    }

    require(rdmaNode.isDefined)
    // Establish a connection to the driver in the background
    if (shouldSendHelloMsg) {
      Future {
        getRdmaChannelToDriver(mustRetry = false)
      }.onSuccess { case rdmaChannel =>
        val buffers = new RdmaShuffleManagerHelloRpcMsg(localRdmaShuffleManagerId.get).
          toRdmaByteBufferManagedBuffers(getRdmaByteBufferManagedBuffer, rdmaShuffleConf.recvWrSize)

        val listener = new RdmaCompletionListener {
          override def onSuccess(buf: ByteBuffer): Unit = buffers.foreach(_.release())
          override def onFailure(e: Throwable): Unit = {
            buffers.foreach(_.release())
            logError("Failed to send RdmaExecutorHelloRpcMsg to driver " + e)
          }
        }

        try {
          rdmaChannel.rdmaSendInQueue(listener, buffers.map(_.getAddress), buffers.map(_.getLkey),
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

    new RdmaShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition,
      endPartition, context)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    // RdmaNode can't be initialized in the c'tor for executors, so the first call will initialize
    startRdmaNodeIfMissing()

    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    // registerShuffle() is only called on the driver, so we let the first caller of getWriter() to
    // initialize the structures for a new ShuffleId
    shuffleBlockResolver.newShuffleWriter(baseShuffleHandle)

    new RdmaWrapperShuffleWriter(shuffleBlockResolver, baseShuffleHandle, mapId, context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    mapTaskOutputsByBlockManagerId.foreach(_._2.remove(shuffleId))
    shuffleBlockResolver.removeShuffle(shuffleId)
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

  private def getRdmaChannel(host: String, port: Int, mustRetry: Boolean): RdmaChannel =
    rdmaNode.get.getRdmaChannel(new InetSocketAddress(host, port), mustRetry)

  def getRdmaChannel(rdmaShuffleManagerId: RdmaShuffleManagerId, mustRetry: Boolean): RdmaChannel =
    getRdmaChannel(rdmaShuffleManagerId.host, rdmaShuffleManagerId.port, mustRetry)

  def getRdmaChannelToDriver(mustRetry: Boolean): RdmaChannel = getRdmaChannel(
    rdmaShuffleConf.driverHost, rdmaShuffleConf.driverPort, mustRetry)

  def getRdmaByteBufferManagedBuffer(length : Int): RdmaByteBufferManagedBuffer =
    new RdmaByteBufferManagedBuffer(new RdmaRegisteredBuffer(rdmaNode.get.getRdmaBufferManager,
      length), length)

  def getRdmaRegisteredBuffer(length : Int): RdmaRegisteredBuffer = new RdmaRegisteredBuffer(
    rdmaNode.get.getRdmaBufferManager, length)

  // TODO: Clean this disni dependency out?
  def getIbvPd: IbvPd = rdmaNode.get.getRdmaBufferManager.getPd

  def getLocalRdmaShuffleManagerId: RdmaShuffleManagerId = localRdmaShuffleManagerId.get

  def putFetchMapStatusCallback(
      callback: (Int, RdmaShuffleManagerId, Seq[RdmaBlockLocation]) => Unit): Int = {
    val callbackId = fetchMapStatusCallbackIndex.getAndIncrement
    val ret = fetchMapStatusCallbackMap.put(callbackId, callback)
    if (ret != null) throw new RuntimeException("Overflow of FetchMapStatusCallbacks")
    callbackId
  }

  def removeFetchMapStatusCallback(callbackId: Int): Unit = fetchMapStatusCallbackMap.remove(
    callbackId)
}
