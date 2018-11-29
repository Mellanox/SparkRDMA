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

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockManagerRemoved}
import org.apache.spark.shuffle.{BaseShuffleHandle, _}
import org.apache.spark.shuffle.rdma.writer.wrapper.RdmaWrapperShuffleWriter
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

private[spark] class RdmaShuffleManager(val conf: SparkConf, isDriver: Boolean)
    extends ShuffleManager with Logging {
  import RdmaShuffleManager._
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
  val blockManagerIdToRdmaShuffleManagerId =
    new ConcurrentHashMap[BlockManagerId, RdmaShuffleManagerId]().asScala
  private val shuffleIdToBufferAddress = new ConcurrentHashMap[ShuffleId, RdmaBuffer]().asScala

  // Used by executor only
  val rdmaShuffleReaderStats: RdmaShuffleReaderStats = {
    if (rdmaShuffleConf.collectShuffleReaderStats) {
      new RdmaShuffleReaderStats(rdmaShuffleConf)
    } else {
      null
    }
  }

  // Mapping from shuffleId to driver's address, length, key of MapOutputLocation buffer
  private val shuffleIdToDriverBufferInfo = new ConcurrentHashMap[ShuffleId, BufferInfo]().asScala

  // Table from shuffleId to RdmaBuffer for mapTaskOutput
  val shuffleIdToMapAddressBuffer =
    new ConcurrentHashMap[ShuffleId, Future[RdmaBuffer]]()

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
            // Since we're reusing executor <-> driver QP - whis will be taken from cache.
            val rdmaChannel = getRdmaChannel(helloMsg.rdmaShuffleManagerId.host,
              helloMsg.channelPort, false, RdmaChannel.RdmaChannelType.RPC)
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

        case announceMsg: RdmaAnnounceRdmaShuffleManagersRpcMsg =>
          // Driver advertises a list of known executor RDMA addresses so connection establishment
          // can be done in the background, before shuffle phases begin
          assume(!isDriver)
          announceMsg.rdmaShuffleManagerIds.filter(_ != localRdmaShuffleManagerId.get).foreach {
            rdmaShuffleManagerId =>
              blockManagerIdToRdmaShuffleManagerId.put(rdmaShuffleManagerId.blockManagerId,
                rdmaShuffleManagerId)
              Future { getRdmaChannel(rdmaShuffleManagerId, mustRetry = false) }
          }
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
      val sc = SparkContext.getOrCreate(conf)
      require(!sc.isLocal, "SparkRDMA shuffle doesn't support local mode.")
      sc.addSparkListener(
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

    // Allocating buffer for table storing map task output (#maps * (address, len, lkey))
    val buffer = getRdmaBufferManager.get(numMaps * RdmaMapTaskOutput.MAP_ENTRY_SIZE)
    logInfo(s"Allocating buffer for shuffleId: $shuffleId MapTask output" +
      s" of size: ${buffer.getLength}")
    shuffleIdToBufferAddress.put(shuffleId, buffer)

    // BypassMergeSortShuffleWriter is not supported since it is package private
    if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new RdmaSerializedShuffleHandle[K, V](buffer.getAddress, buffer.getLength, buffer.getLkey,
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new RdmaBaseShuffleHandle(buffer.getAddress, buffer.getLength, buffer.getLkey,
        shuffleId, numMaps, dependency)
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
        getRdmaChannelToDriver(mustRetry = true)
      }.onSuccess { case rdmaChannel =>
        val port = rdmaChannel.getSourceSocketAddress.getPort
        val buffers = new RdmaShuffleManagerHelloRpcMsg(localRdmaShuffleManagerId.get, port).
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
      // Pre allocate buffers in parallel outside of synchronized block to avoid thread contention
      rdmaShuffleConf.preAllocateBuffers.par.foreach{
        case (buffSize, buffCount) => getRdmaBufferManager.preAllocate(buffSize, buffCount)
      }
    }
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    // RdmaNode can't be initialized in the c'tor for executors, so the first call will initialize
    startRdmaNodeIfMissing()

    shuffleIdToDriverBufferInfo.putIfAbsent(handle.shuffleId, {
      val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
      if (handle.isInstanceOf[RdmaBaseShuffleHandle[_, _, _]]) {
        val rdmaBaseShuffleHandle = baseShuffleHandle.asInstanceOf[RdmaBaseShuffleHandle[K, _, C]]
        BufferInfo(
          rdmaBaseShuffleHandle.driverTableAddress,
          rdmaBaseShuffleHandle.driverTableLength,
          rdmaBaseShuffleHandle.driverTableRKey
        )
      } else {
        val rdmaSerializedShuffle =
          baseShuffleHandle.asInstanceOf[RdmaSerializedShuffleHandle[K, C]]
        BufferInfo(
          rdmaSerializedShuffle.driverTableAddress,
          rdmaSerializedShuffle.driverTableLength,
          rdmaSerializedShuffle.driverTableRKey
        )
      }
    })

    new RdmaShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition,
      endPartition, context)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    // RdmaNode can't be initialized in the c'tor for executors, so the first call will initialize
    startRdmaNodeIfMissing()
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    shuffleIdToDriverBufferInfo.putIfAbsent(handle.shuffleId, {
      if (handle.isInstanceOf[RdmaBaseShuffleHandle[_, _, _]]) {
        val rdmaBaseShuffleHandle = baseShuffleHandle.asInstanceOf[RdmaBaseShuffleHandle[K, V, _]]
        BufferInfo(
          rdmaBaseShuffleHandle.driverTableAddress,
          rdmaBaseShuffleHandle.driverTableLength,
          rdmaBaseShuffleHandle.driverTableRKey
        )
      } else {
        val rdmaSerializedShuffle =
          baseShuffleHandle.asInstanceOf[RdmaSerializedShuffleHandle[K, V]]
        BufferInfo(
          rdmaSerializedShuffle.driverTableAddress,
          rdmaSerializedShuffle.driverTableLength,
          rdmaSerializedShuffle.driverTableRKey
        )
      }
    })
    // registerShuffle() is only called on the driver, so we let the first caller of getWriter() to
    // initialize the structures for a new ShuffleId
    shuffleBlockResolver.newShuffleWriter(baseShuffleHandle)

    new RdmaWrapperShuffleWriter(shuffleBlockResolver, baseShuffleHandle, mapId, context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    mapTaskOutputsByBlockManagerId.foreach(_._2.remove(shuffleId))
    shuffleBlockResolver.removeShuffle(shuffleId)
    shuffleIdToBufferAddress.remove(shuffleId).map(_.free())
    shuffleIdToDriverBufferInfo.remove(shuffleId)
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

  private def getRdmaChannel(host: String, port: Int, mustRetry: Boolean,
      rdmaChannelType: RdmaChannel.RdmaChannelType): RdmaChannel =
    rdmaNode.get.getRdmaChannel(new InetSocketAddress(host, port), mustRetry, rdmaChannelType)

  def getRdmaChannel(rdmaShuffleManagerId: RdmaShuffleManagerId,
      mustRetry: Boolean): RdmaChannel = {
    getRdmaChannel(rdmaShuffleManagerId.host, rdmaShuffleManagerId.port, mustRetry,
      RdmaChannel.RdmaChannelType.RDMA_READ_REQUESTOR)
  }

  def getRdmaChannelToDriver(mustRetry: Boolean): RdmaChannel = getRdmaChannel(
    rdmaShuffleConf.driverHost, rdmaShuffleConf.driverPort, mustRetry,
    RdmaChannel.RdmaChannelType.RPC)

  def getRdmaBufferManager: RdmaBufferManager = rdmaNode.get.getRdmaBufferManager

  def getRdmaByteBufferManagedBuffer(length : Int): RdmaByteBufferManagedBuffer =
    new RdmaByteBufferManagedBuffer(new RdmaRegisteredBuffer(getRdmaBufferManager, length), length)

  def getRdmaRegisteredBuffer(length : Int): RdmaRegisteredBuffer = new RdmaRegisteredBuffer(
    getRdmaBufferManager, length)

  def getLocalRdmaShuffleManagerId: RdmaShuffleManagerId = localRdmaShuffleManagerId.get

  /**
   * Retrieves on each executor MapTaskOutputTable from driver.
   * @param shuffleId
   * @return
   */
  def getMapTaskOutputTable(shuffleId: ShuffleId): Future[RdmaBuffer] = {
    shuffleIdToMapAddressBuffer.computeIfAbsent(shuffleId,
      new java.util.function.Function[ShuffleId, Future[RdmaBuffer]] {
       override def apply(v1: ShuffleId): Future[RdmaBuffer] = {
         val result = Promise[RdmaBuffer]
         val startTime = System.currentTimeMillis()
         val BufferInfo(rAddress, rLength, rKey) = shuffleIdToDriverBufferInfo(shuffleId)
         val mapTaskOutputBuffer = getRdmaBufferManager.get(rLength)
         val channel = getRdmaChannelToDriver(true)
         val listener = new RdmaCompletionListener {
           override def onSuccess(buf: ByteBuffer): Unit = {
             result.complete(Success(mapTaskOutputBuffer))
             logInfo(s"RDMA read mapTaskOutput table for shuffleId: $shuffleId " +
               s"took ${Utils.getUsedTimeMs(startTime)}")
           }

           override def onFailure(exception: Throwable): Unit = {
             logError(s"Failed to RDMA read MapTaskOutput from driver")
             result.complete(Failure(exception))
           }

         }
         val addresses = Array(rAddress)
         val sizes = Array(rLength)
         val rKeys = Array(rKey)
         logInfo(s"Getting MapTaskOutput table for shuffleId: $shuffleId " +
           s"of size $rLength from driver")
         channel.rdmaReadInQueue(
           listener, mapTaskOutputBuffer.getAddress, mapTaskOutputBuffer.getLkey,
           sizes, addresses, rKeys)

         result.future
       }
      })
    shuffleIdToMapAddressBuffer.get(shuffleId)
  }

  /**
   * Doing RDMA write of MapTaskOutput buffer to driver at position of mapId*ENTRY_SIZE
   * @param shuffleId
   * @param mapId
   * @param mapTaskOutput
   */
  def publishMapTaskOutput(shuffleId: ShuffleId, mapId: MapId,
      mapTaskOutput: RdmaMapTaskOutput): Future[Boolean] = {
    assume(!isDriver)
    val result = Promise[Boolean]
    val rdmaBuffer = getRdmaBufferManager.get(RdmaMapTaskOutput.MAP_ENTRY_SIZE)
    val buf = rdmaBuffer.getByteBuffer
    buf.putLong(mapTaskOutput.getRdmaBuffer.getAddress)
    buf.putInt(mapTaskOutput.getRdmaBuffer.getLkey)

    val BufferInfo(driverTableAddress, _, driverTableKey) = shuffleIdToDriverBufferInfo(shuffleId)
    val startTime = System.currentTimeMillis()
    val writeListener = new RdmaCompletionListener {
      override def onSuccess(buf: ByteBuffer): Unit = {
        logInfo(s"RDMA write map task output for mapId: $mapId to driver " +
          s"took ${Utils.getUsedTimeMs(startTime)}")
        getRdmaBufferManager.put(rdmaBuffer)
        result.complete(Success(true))
      }

      override def onFailure(exception: Throwable): Unit = {
        logError(s"Fail to RDMA write MapTaskOutput address: " +
          s"${exception.getLocalizedMessage}")
        getRdmaBufferManager.put(rdmaBuffer)
        result.complete(Failure(exception))
      }
    }
    getRdmaChannelToDriver(true).rdmaWriteInQueue(writeListener,
      rdmaBuffer.getAddress, RdmaMapTaskOutput.MAP_ENTRY_SIZE, rdmaBuffer.getLkey,
      driverTableAddress + mapId * RdmaMapTaskOutput.MAP_ENTRY_SIZE, driverTableKey)

    result.future.onFailure{
      case ex: Exception => throw ex
    }
    result.future
  }
}

object RdmaShuffleManager{
  type ShuffleId = Int
  type MapId = Int

  // Information needed to do RDMA read of remote buffer
  case class BufferInfo(address: Long, length: Int, rKey: Int) {
    require(length >= 0)
  }
}
