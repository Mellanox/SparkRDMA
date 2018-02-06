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
import java.util.Timer
import java.util.concurrent.{ConcurrentLinkedDeque, LinkedBlockingQueue}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

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
  private[this] val pendingFetchesQueue = new ConcurrentLinkedDeque[PendingFetch]()

  private[this] val rdmaShuffleReaderStats = rdmaShuffleManager.rdmaShuffleReaderStats

  initialize()

  private[this] def cleanup() {
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
          s" start partition: $startPartition  from " +
          s"${pendingFetch.rdmaShuffleManagerId.blockManagerId} " +
          s"after ${Utils.getUsedTimeMs(startRemoteFetchTime)}")
      }

      override def onFailure(e: Throwable): Unit = {
        logError("Failed to getRdmaBlockLocation block(s) from: " +
          pendingFetch.rdmaShuffleManagerId.blockManagerId + ", Exception: " + e)
        resultsQueue.put(FailureFetchResult(startPartition,
            pendingFetch.rdmaShuffleManagerId.blockManagerId, e))

        rdmaByteBufferManagedBuffers.foreach(_.release())
        // We skip curBytesInFlight since we expect one failure to fail the whole task
      }
    }

    try {
      val rdmaChannel = rdmaShuffleManager.getRdmaChannel(pendingFetch.rdmaShuffleManagerId,
        mustRetry = true)
      rdmaChannel.rdmaReadInQueue(listener, rdmaRegisteredBuffer.getRegisteredAddress,
        rdmaRegisteredBuffer.getLkey, pendingFetch.rdmaBlockLocations.map(_.length).toArray,
        pendingFetch.rdmaBlockLocations.map(_.address).toArray,
        pendingFetch.rdmaBlockLocations.map(_.mKey).toArray)
    } catch {
      case e: Exception => listener.onFailure(e)
    }
  }

  private[this] def startAsyncRemoteFetches(): Unit = {
    val startRemotePartitionLocationFetch = System.currentTimeMillis()

    val groupedBlocksByAddress = blocksByAddress.filter(_._1 != localBlockManagerId).map {
      case (blockManagerId, seq) => (blockManagerId, seq.filter(_._2 > 0))}.filter(_._2.nonEmpty)

    val totalRemainingLocations = new AtomicInteger(groupedBlocksByAddress.map(_._2.length).sum)

    if (totalRemainingLocations.get() > 0) {
      def metadataFetchFailed(e: Throwable) = resultsQueue.put(FailureMetadataFetchResult(
        new MetadataFetchFailedException(shuffleId, startPartition,
          "Failed to fetch remote partition locations for ShuffleId: " + shuffleId +
            " PartitionId: " + startPartition + " from driver: " + e)))

      val timerTask = new java.util.TimerTask {
        override def run(): Unit = metadataFetchFailed(new TimeoutException("Timed-out while " +
          "fetching remote partition locations for ShuffleId: " + shuffleId + " PartitionIds: " +
          (startPartition until endPartition).mkString(", ") + " from driver, consider " +
          "increasing the value of spark.shuffle.rdma.partitionLocationFetchTimeout " +
          "(current value: " + rdmaShuffleConf.partitionLocationFetchTimeout + ")"))
      }

      groupedBlocksByAddress.foreach { case (blockManagerId, blockIdSeq) =>
        val remainingLocations = new AtomicInteger(blockIdSeq.length)
        val resolvedLocationList = new ListBuffer[RdmaBlockLocation]

        // Create a callback for handling FetchMapStatusResponse messages
        val responseCallback = (callbackId: Int,
            requestedRdmaShuffleManagerId: RdmaShuffleManagerId,
            locations: Seq[RdmaBlockLocation]) => {
          resolvedLocationList.synchronized { resolvedLocationList ++= locations }

          if (remainingLocations.addAndGet(-locations.length) == 0) {
            // Once all of the partitions locations are ready, map them into PendingFetches
            logInfo(s"Fetching remote partition locations for ${blockManagerId.executorId}" +
              s" took ${Utils.getUsedTimeMs(startRemotePartitionLocationFetch)}")
            // Callback is no longer necessary, remove it
            rdmaShuffleManager.removeFetchMapStatusCallback(callbackId)

            // Build PendingFetches our of the RdmaBlockLocations
            val allPendingFetches = {
              val pendingFetches = new ListBuffer[PendingFetch]
              var curRdmaBlockLocations = new ListBuffer[RdmaBlockLocation]
              var totalLength: Int = 0

              for (rdmaBlockLocation <- resolvedLocationList) {
                // Group RdmaBlockLocations up to rdmaShuffleConf.shuffleReadBlockSize
                if (totalLength + rdmaBlockLocation.length <=
                    rdmaShuffleConf.shuffleReadBlockSize) {
                  totalLength += rdmaBlockLocation.length
                } else {
                  if (totalLength > 0) {
                    pendingFetches += PendingFetch(requestedRdmaShuffleManagerId,
                      curRdmaBlockLocations, totalLength)
                  }
                  totalLength = rdmaBlockLocation.length
                  curRdmaBlockLocations = new ListBuffer[RdmaBlockLocation]
                }
                curRdmaBlockLocations += rdmaBlockLocation
              }
              if (totalLength > 0) {
                pendingFetches += PendingFetch(requestedRdmaShuffleManagerId, curRdmaBlockLocations,
                  totalLength)
              }

              pendingFetches
            }
            for (pendingFetch <- allPendingFetches) {
              numBlocksToFetch.addAndGet(pendingFetch.rdmaBlockLocations.length)

              // Start the fetch if no more than rdmaShuffleConf.maxBytesInFlight are in progress
              if (curBytesInFlight.get() < rdmaShuffleConf.maxBytesInFlight) {
                curBytesInFlight.addAndGet(pendingFetch.totalLength)
                Future { fetchBlocks(pendingFetch) }
              } else {
                pendingFetchesQueue.add(pendingFetch)
              }
            }
          }

          if (totalRemainingLocations.addAndGet(-locations.length) == 0) {
            // Once all of the expected RdmaBlockLocations arrived, stop the timeout timer and kick
            // off the the dummy result so that the iterator can quit if there are no more pending
            // blocks
            timerTask.cancel()
            insertDummyResult()
          }
        }: Unit
        val callbackId = rdmaShuffleManager.putFetchMapStatusCallback(responseCallback)

        // Filter out non-shuffle blocks - they are unexpected
        val mapIdReduceIds = blockIdSeq.filter(_._1.isShuffle).map(
          _._1.asInstanceOf[ShuffleBlockId]).map( blockId => (blockId.mapId, blockId.reduceId))

        val rdmaChannel = rdmaShuffleManager.getRdmaChannelToDriver(mustRetry = true)
        try {
          val buffers = new RdmaFetchMapStatusRpcMsg(
            rdmaShuffleManager.getLocalRdmaShuffleManagerId, blockManagerId, shuffleId, callbackId,
            mapIdReduceIds).toRdmaByteBufferManagedBuffers(
              rdmaShuffleManager.getRdmaByteBufferManagedBuffer, rdmaShuffleConf.recvWrSize)

          val listener = new RdmaCompletionListener {
            override def onSuccess(buf: ByteBuffer): Unit = buffers.foreach(_.release())
            override def onFailure(e: Throwable): Unit = {
              buffers.foreach(_.release())
              logError("Failed to send RdmaFetchMapStatusRpcMsg to " + rdmaChannel + ":" +
                ", Exception: " + e)
            }
          }

          try {
            rdmaChannel.rdmaSendInQueue(listener, buffers.map(_.getAddress), buffers.map(_.getLkey),
              buffers.map(_.getLength.toInt))
          } catch {
            case e: Exception =>
              listener.onFailure(e)
              throw e
          }
        } catch {
          case e: Exception =>
            timerTask.cancel()
            metadataFetchFailed(e)
        }
      }

      // Start the timeout timer once all messages are submitted
      try {
        new Timer(true).schedule(timerTask, rdmaShuffleConf.partitionLocationFetchTimeout)
      } catch {
        // Covers the case where timerTask was already cancelled, no further action is needed
        case _: IllegalStateException =>
      }
    } else {
      // No RdmaBlockLocations are expected. Kick off the the dummy result so that the iterator can
      // quit if there are no more pending blocks
      insertDummyResult()
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
      pendingFetchesQueue.pollFirst() match {
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
