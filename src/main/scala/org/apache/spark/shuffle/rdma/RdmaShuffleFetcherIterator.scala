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

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.{Comparator, NoSuchElementException}
import java.util.concurrent.{LinkedBlockingQueue, PriorityBlockingQueue}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{FetchFailedException, MetadataFetchFailedException}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.Utils

private[spark] final class RdmaShuffleFetcherIterator(
    context: TaskContext,
    startPartition: Int,
    endPartition: Int,
    shuffleId : Int,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])])
  extends Iterator[InputStream] with Logging {
  import org.apache.spark.shuffle.rdma.RdmaShuffleFetcherIterator._

  // numBlocksToFetch is initialized with "1" so hasNext() will return true until all of the remote
  // fetches has been started. The remaining extra "1" will be fulfilled with a null InputStream in
  // insertDummyResult()
  private[this] val numBlocksToFetch = new AtomicInteger(1)
  private[this] var numBlocksProcessed = 0

  private[this] val rdmaShuffleManager =
    SparkEnv.get.shuffleManager.asInstanceOf[RdmaShuffleManager]

  private[this] val resultsQueue = new LinkedBlockingQueue[FetchResult]

  @volatile private[this] var currentResult: FetchResult = _

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  @volatile private[this] var isStopped = false

  private[this] val localBlockManagerId = SparkEnv.get.blockManager.blockManagerId
  private[this] val rdmaShuffleConf = rdmaShuffleManager.rdmaShuffleConf

  private[this] val curBytesInFlight = new AtomicLong(0)

  case class PendingFetch(
    rdmaShuffleManagerId: RdmaShuffleManagerId,
    rdmaBlockLocations: Seq[RdmaBlockLocation],
    totalLength: Int)

  val rand = new scala.util.Random(System.nanoTime())
  // Make random ordering of pending fetches to prevent oversubscription to channel
  private[this] val pendingFetchesQueue = new PriorityBlockingQueue[PendingFetch](100,
    new Comparator[PendingFetch] {
      override def compare(o1: PendingFetch, o2: PendingFetch): Int = -1 + rand.nextInt(3)
    })

  private[this] val rdmaShuffleReaderStats = rdmaShuffleManager.rdmaShuffleReaderStats
  private[this] val rdmaReadRequestsLimit = rdmaShuffleConf.sendQueueDepth /
    rdmaShuffleConf.getConfKey("spark.executor.cores", "1").toInt

  initialize()

  private[this] def cleanup() {
    logDebug(s"ShuffleId: $shuffleId, reduceId: ${context.partitionId()}\n" +
            s"Number blocks processed: $numBlocksProcessed,\n" +
            s"total fetch wait time: ${shuffleMetrics.fetchWaitTime}")
    isStopped = true
    currentResult match {
      case SuccessFetchResult(_, _, inputStream) if inputStream != null => inputStream.close()
      case _ =>
    }
    currentResult = null

    val iter = resultsQueue.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, blockManagerId, inputStream) if inputStream != null =>
          if (blockManagerId != localBlockManagerId) {
            shuffleMetrics.incRemoteBytesRead(inputStream.available)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          inputStream.close()
        case _ =>
      }
    }
  }

  private[this] def insertDummyResult(): Unit = {
    if (!isStopped) {
      resultsQueue.put(SuccessFetchResult(startPartition, localBlockManagerId, null))
    }
  }

  private[this] def fetchBlocks(pendingFetch: PendingFetch): Unit = {
    val startRemoteFetchTime = System.currentTimeMillis()
    var rdmaRegisteredBuffer: RdmaRegisteredBuffer = null
    val rdmaByteBufferManagedBuffers = try {
      // Allocate a buffer and multiple ByteBuffers for the incoming data while connection is being
      // established/retrieved
      rdmaRegisteredBuffer = rdmaShuffleManager.getRdmaRegisteredBuffer(pendingFetch.totalLength)

      pendingFetch.rdmaBlockLocations.map(_.length).map(
        new RdmaByteBufferManagedBuffer(rdmaRegisteredBuffer, _))
    } catch {
      case e: Exception =>
        if (rdmaRegisteredBuffer != null) rdmaRegisteredBuffer.release()
        logError("Failed to allocate memory for incoming block fetches, failing pending" +
          " block fetches. " + e)
        resultsQueue.put(FailureFetchResult(startPartition, null, e))

        return
    }

    val listener = new RdmaCompletionListener {
      override def onSuccess(paramBuf: ByteBuffer): Unit = {
          rdmaByteBufferManagedBuffers.foreach { buf =>
            if (!isStopped) {
              val inputStream = new BufferReleasingInputStream(buf.createInputStream(), buf)
              resultsQueue.put(SuccessFetchResult(startPartition,
                pendingFetch.rdmaShuffleManagerId.blockManagerId, inputStream))
            } else {
              buf.release()
            }
          }
          if (rdmaShuffleReaderStats != null) {
            rdmaShuffleReaderStats.updateRemoteFetchHistogram(
              pendingFetch.rdmaShuffleManagerId.blockManagerId,
              (System.currentTimeMillis() - startRemoteFetchTime).toInt)
          }
        logTrace(s"Got remote block(s): of size ${pendingFetch.totalLength} " +
          s"from ${pendingFetch.rdmaShuffleManagerId.blockManagerId} " +
          s"after ${Utils.getUsedTimeMs(startRemoteFetchTime)}")
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"Failed to getRdmaBlockLocation block(s) from: " +
          s"${pendingFetch.rdmaShuffleManagerId.blockManagerId},\n Exception: $e")
        resultsQueue.put(FailureFetchResult(startPartition,
            pendingFetch.rdmaShuffleManagerId.blockManagerId, e))

        rdmaByteBufferManagedBuffers.foreach(_.release())
        // We skip curBytesInFlight since we expect one failure to fail the whole task
      }
    }

    try {
      val rdmaChannel = rdmaShuffleManager.getRdmaChannel(
        pendingFetch.rdmaShuffleManagerId, mustRetry = true)
      rdmaChannel.rdmaReadInQueue(listener, rdmaRegisteredBuffer.getRegisteredAddress,
        rdmaRegisteredBuffer.getLkey, pendingFetch.rdmaBlockLocations.map(_.length).toArray,
        pendingFetch.rdmaBlockLocations.map(_.address).toArray,
        pendingFetch.rdmaBlockLocations.map(_.mKey).toArray)
    } catch {
      case e: Exception => listener.onFailure(e)
    }
  }

  private[this] def startAsyncRemoteFetches(): Unit = {
    // 1. Get the whole MapTaskOutputAddressTable
    rdmaShuffleManager.getMapTaskOutputTable(shuffleId).onComplete{
      case Failure(ex) => resultsQueue.put(FailureMetadataFetchResult(
        new MetadataFetchFailedException(shuffleId, context.partitionId(), ex.getMessage)))
        logError(s"Failed to RDMA read MapTaskOutputAddressTable: $ex")
      case Success(rdmaBuffer) =>
      val mapTaskOutput = rdmaBuffer.getByteBuffer
      val groupedBlocksByAddress = rand.shuffle(
        blocksByAddress.filter(_._1 != localBlockManagerId).map {
          case (blockManagerId, seq) => (blockManagerId, seq.filter(_._2 > 0))
        }.filter(_._2.nonEmpty))

      val totalRemainingLocations = new AtomicInteger(groupedBlocksByAddress.map(_._2.length).sum)
      if (totalRemainingLocations.get() == 0) {
        // No RdmaBlockLocations are expected. Kick off the the dummy result
        // so that the iterator can quit if there are no more pending blocks
        insertDummyResult()
      }

      groupedBlocksByAddress.foreach { case (blockManagerId, blocks) =>
        var requestedRdmaShuffleManagerId: RdmaShuffleManagerId = null
        try {
          requestedRdmaShuffleManagerId =
            rdmaShuffleManager.blockManagerIdToRdmaShuffleManagerId(blockManagerId)
        } catch {
          case _: NoSuchElementException =>
            val error = s"RdmaShuffleNode: ${localBlockManagerId} " +
              s"has no RDMA connection to $blockManagerId"
            logError(error)
            resultsQueue.put(FailureMetadataFetchResult(
              new MetadataFetchFailedException(shuffleId, context.partitionId(), error)))
            return
        }

        blocks.grouped(rdmaReadRequestsLimit).foreach {
          case blockIdSeq =>
            // Filter out non-shuffle blocks - they are unexpected
            val shuffleBlocks = blockIdSeq.withFilter(_._1.isShuffle).map(
              _._1.asInstanceOf[ShuffleBlockId]).toArray
            val shuffleBlocksCount = shuffleBlocks.size

            val localBlockLocationBuffer = rdmaShuffleManager.getRdmaBufferManager.get(
              RdmaMapTaskOutput.ENTRY_SIZE * shuffleBlocksCount)

            val startTime = System.currentTimeMillis()
            val executorRdmaReadBlockLocationListener = new RdmaCompletionListener {
              override def onSuccess(buf: ByteBuffer): Unit = {
                // 3. Finally we have RdmaBlockLocation for this mapId/reduceId. Let's fetch it.
                logTrace(s"RDMA read ${shuffleBlocksCount} block locations " +
                  s"from ${requestedRdmaShuffleManagerId.blockManagerId} took: " +
                  s"${Utils.getUsedTimeMs(startTime)}")
                val blockLocationBuffer = localBlockLocationBuffer.getByteBuffer
                var totalLength = 0
                var totalReadRequests = 0
                var curRdmaBlockLocations = new ListBuffer[RdmaBlockLocation]
                val pendingFetches = new ListBuffer[PendingFetch]
                (0 until shuffleBlocksCount).foreach { _ =>
                  val rdmaBlockLocation = RdmaBlockLocation(blockLocationBuffer.getLong(),
                    blockLocationBuffer.getInt(), blockLocationBuffer.getInt())

                  if (totalLength + rdmaBlockLocation.length <= rdmaShuffleConf.shuffleReadBlockSize
                    && totalReadRequests < rdmaReadRequestsLimit) {
                    totalLength += rdmaBlockLocation.length
                    totalReadRequests += 1
                  } else {
                    if (totalLength > 0) {
                      pendingFetches += PendingFetch(requestedRdmaShuffleManagerId,
                        curRdmaBlockLocations, totalLength)
                    }
                    totalLength = rdmaBlockLocation.length
                    totalReadRequests = 1
                    curRdmaBlockLocations = new ListBuffer[RdmaBlockLocation]
                  }
                  curRdmaBlockLocations += rdmaBlockLocation
                }
                localBlockLocationBuffer.free()
                if (totalLength > 0) {
                  pendingFetches += PendingFetch(requestedRdmaShuffleManagerId,
                    curRdmaBlockLocations, totalLength)
                }
                for (pendingFetch <- pendingFetches) {
                  // Start fetch if no more than rdmaShuffleConf.maxBytesInFlight are in progress
                  numBlocksToFetch.addAndGet(pendingFetch.rdmaBlockLocations.length)
                  if (curBytesInFlight.get() < rdmaShuffleConf.maxBytesInFlight) {
                    curBytesInFlight.addAndGet(pendingFetch.totalLength)
                    Future { fetchBlocks(pendingFetch) }
                  } else {
                    pendingFetchesQueue.add(pendingFetch)
                  }
                }
                if (totalRemainingLocations.addAndGet(-shuffleBlocksCount) == 0) {
                  insertDummyResult()
                }
              }
              override def onFailure(exception: Throwable): Unit = {
                val mapOutputBuf = rdmaShuffleManager.shuffleIdToMapAddressBuffer
                  .remove(shuffleId)
                if (mapOutputBuf != null) {
                  mapOutputBuf.map(_.free())
                }
                localBlockLocationBuffer.free()
                resultsQueue.put(FailureMetadataFetchResult(
                  new MetadataFetchFailedException(shuffleId, context.partitionId(),
                    exception.getLocalizedMessage)))
                logError(s"Failed to RDMA read ${shuffleBlocksCount} block locations " +
                    s"from ${requestedRdmaShuffleManagerId.blockManagerId}: \n $exception")
              }
            }

            val rAddresses = new Array[Long](shuffleBlocks.size)
            val rSizes = Array.fill(shuffleBlocks.size)(RdmaMapTaskOutput.ENTRY_SIZE)
            val rKeys = new Array[Int](shuffleBlocks.size)

            shuffleBlocks.zipWithIndex.foreach {
              case (shuffleBlock, i) =>
                // 2.RDMA read from other executor MapTaskOutput portion itself for mapId-reduceID
                val mapIdOffset = shuffleBlock.mapId * RdmaMapTaskOutput.MAP_ENTRY_SIZE
                val reduceIdOffset = mapTaskOutput.getLong(mapIdOffset) +
                  shuffleBlock.reduceId * RdmaMapTaskOutput.ENTRY_SIZE
                rAddresses(i) = reduceIdOffset
                rKeys(i) = mapTaskOutput.getInt(mapIdOffset + 8)
            }
            try {
              val channelToExecutor =
                rdmaShuffleManager.getRdmaChannel(requestedRdmaShuffleManagerId, true)
              channelToExecutor.rdmaReadInQueue(executorRdmaReadBlockLocationListener,
                localBlockLocationBuffer.getAddress, localBlockLocationBuffer.getLkey,
                rSizes, rAddresses, rKeys)
            } catch {
              case ex: Exception =>
                executorRdmaReadBlockLocationListener.onFailure(ex)
            }
        }
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    startAsyncRemoteFetches()

    for (partitionId <- startPartition until endPartition) {
      rdmaShuffleManager.shuffleBlockResolver.getLocalRdmaPartition(shuffleId, partitionId).foreach{
        case in: InputStream =>
          shuffleMetrics.incLocalBlocksFetched(1)
          shuffleMetrics.incLocalBytesRead(in.available())
          resultsQueue.put(SuccessFetchResult(partitionId, localBlockManagerId, in))

          numBlocksToFetch.incrementAndGet()
        case _ =>
      }
    }
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch.get()

  override def next(): InputStream = {
    if (!hasNext) {
      return null
    }

    numBlocksProcessed += 1

    val startFetchWait = System.currentTimeMillis()
    currentResult = resultsQueue.take()
    val result = currentResult
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    result match {
      case SuccessFetchResult(_, blockManagerId, inputStream) =>
        if (inputStream != null && blockManagerId != localBlockManagerId) {
          shuffleMetrics.incRemoteBytesRead(inputStream.available)
          shuffleMetrics.incRemoteBlocksFetched(1)
          curBytesInFlight.addAndGet(-inputStream.available)
        }
      case _ =>
    }

    // Start some pending remote fetches
    while (!pendingFetchesQueue.isEmpty &&
        curBytesInFlight.get() < rdmaShuffleConf.maxBytesInFlight) {
      pendingFetchesQueue.poll() match {
        case pendingFetch: PendingFetch =>
          curBytesInFlight.addAndGet(pendingFetch.totalLength)
          Future { fetchBlocks(pendingFetch) }
        case _ =>
      }
    }

    result match {
      case FailureMetadataFetchResult(e) => throw e
      case FailureFetchResult(partitionId, blockManagerId, e) =>
        throw new FetchFailedException(blockManagerId, shuffleId, 0, partitionId, e)
      case SuccessFetchResult(_, _, inputStream) => inputStream
    }
  }

  override def toString(): String = {
    s"RdmaShuffleFetchIterator(shuffleId: $shuffleId, startPartition: $startPartition," +
      s"endPartition: $endPartition)"
  }
}

private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val buf: ManagedBuffer)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      buf.release()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[rdma]
object RdmaShuffleFetcherIterator {

  private[rdma] sealed trait FetchResult { }

  private[rdma] case class SuccessFetchResult(
      partitionId: Int,
      blockManagerId: BlockManagerId,
      inputStream: InputStream) extends FetchResult

  private[rdma] case class FailureFetchResult(
      partitionId: Int,
      blockManagerId: BlockManagerId,
      e: Throwable) extends FetchResult

  private[rdma] case class FailureMetadataFetchResult(e: MetadataFetchFailedException)
      extends FetchResult
}
