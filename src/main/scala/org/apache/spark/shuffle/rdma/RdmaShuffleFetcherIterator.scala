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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedDeque, LinkedBlockingQueue}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{FetchFailedException, MetadataFetchFailedException}
import org.apache.spark.util.Utils

private[spark] final class RdmaShuffleFetcherIterator(
    context: TaskContext,
    startPartition: Int,
    endPartition: Int,
    shuffleId : Int)
  extends Iterator[InputStream] with Logging {
  import org.apache.spark.shuffle.rdma.RdmaShuffleFetcherIterator._

  private[this] val startTime = System.currentTimeMillis

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

  @GuardedBy("this")
  private[this] var isStopped = false

  private[this] val localRdmaShuffleManagerId = rdmaShuffleManager.getLocalRdmaShuffleManagerId
  private[this] val rdmaShuffleConf = rdmaShuffleManager.rdmaShuffleConf

  private[this] val maxBytesInFlight = rdmaShuffleConf.maxBytesInFlight
  private[this] val curBytesInFlight = new AtomicLong(0)

  case class AggregatedPartitionGroup(
    var totalLength: Int,
    locations: ListBuffer[RdmaBlockLocation])

  case class PendingFetch(
    rdmaShuffleManagerId: RdmaShuffleManagerId,
    aggregatedPartitionGroup: AggregatedPartitionGroup)
  private[this] val pendingFetchesQueue = new ConcurrentLinkedDeque[PendingFetch]()

  private[this] val rdmaShuffleReaderStats = rdmaShuffleManager.rdmaShuffleReaderStats

  initialize()

  private[this] def cleanup() {
    synchronized { isStopped = true }

    currentResult match {
      case SuccessFetchResult(_, _, inputStream) if inputStream != null => inputStream.close()
      case _ =>
    }
    currentResult = null

    val iter = resultsQueue.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, rdmaShuffleManagerId, inputStream) if inputStream != null =>
          if (rdmaShuffleManagerId != localRdmaShuffleManagerId) {
            shuffleMetrics.incRemoteBytesRead(inputStream.available)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          inputStream.close()
        case _ =>
      }
    }
  }

  private[this] def wrapFuturesWithTimeout[T](futureList: Seq[Future[T]], timeoutMs: Int)
      (implicit ec: ExecutionContext): Future[Seq[T]] = {
    val promise = Promise[Seq[T]]()
    val timerTask = new java.util.TimerTask {
      override def run(): Unit = promise.failure(new TimeoutException())
    }

    val timer = new Timer(true)
    timer.schedule(timerTask, timeoutMs)

    val combinedFuture = Future.sequence(futureList)
    combinedFuture.onComplete( _ => timerTask.cancel() )

    Future.firstCompletedOf(List(combinedFuture, promise.future))
  }

  private[this] def insertDummyResult(): Unit = {
    RdmaShuffleFetcherIterator.this.synchronized {
      if (!isStopped) {
        resultsQueue.put(SuccessFetchResult(startPartition, localRdmaShuffleManagerId, null))
      }
    }
  }

  private[this] def fetchBlocks(
      rdmaShuffleManagerId: RdmaShuffleManagerId,
      aggregatedPartitionGroup: AggregatedPartitionGroup): Unit = {
    val startRemoteFetchTime = System.currentTimeMillis()

    val rdmaChannel = try {
      rdmaShuffleManager.getRdmaChannel(rdmaShuffleManagerId.host, rdmaShuffleManagerId.port, true)
    } catch {
      case e: Exception =>
        logError("Failed to establish a connection to rdmaShuffleManager: " + rdmaShuffleManagerId +
          ", failing pending block fetches. " + e)
        RdmaShuffleFetcherIterator.this.synchronized {
          resultsQueue.put(FailureFetchResult(startPartition, rdmaShuffleManagerId, e))
        }
        return
    }

    var rdmaRegisteredBuffer: RdmaRegisteredBuffer = null
    val rdmaByteBufferManagedBuffers = try {
      // Allocate a buffer and multiple ByteBuffers for the incoming data while connection is being
      // established/retrieved
      rdmaRegisteredBuffer = rdmaShuffleManager.getRdmaRegisteredBuffer(
        aggregatedPartitionGroup.totalLength)

      aggregatedPartitionGroup.locations.map(_.length).map(
        new RdmaByteBufferManagedBuffer(rdmaRegisteredBuffer, _))
    } catch {
      case e: Exception =>
        if (rdmaRegisteredBuffer != null) rdmaRegisteredBuffer.release()
        logError("Failed to allocate memory for incoming block fetches, failing pending" +
          " block fetches. " + e)
        RdmaShuffleFetcherIterator.this.synchronized {
          resultsQueue.put(FailureFetchResult(startPartition, rdmaShuffleManagerId, e))
        }
        return
    }

    val listener = new RdmaCompletionListener {
      override def onSuccess(paramBuf: ByteBuffer): Unit = {
        RdmaShuffleFetcherIterator.this.synchronized {
          rdmaByteBufferManagedBuffers.foreach { buf =>
            if (!isStopped) {
              val inputStream = new BufferReleasingInputStream(buf.createInputStream(), buf)
              // TODO: startPartition may only be one of the partitions to report
              RdmaShuffleFetcherIterator.this.synchronized {
                resultsQueue.put(SuccessFetchResult(
                  startPartition,
                  rdmaShuffleManagerId,
                  inputStream))
              }
            } else {
              buf.release()
            }
          }
          if (rdmaShuffleReaderStats != null) {
            rdmaShuffleReaderStats.updateRemoteFetchHistogram(rdmaShuffleManagerId,
              (System.currentTimeMillis() - startRemoteFetchTime).toInt)
          }
        }
        // TODO: startPartition may only be one of the partitions to report
        logTrace("Got remote block(s) " + startPartition + " from " + rdmaShuffleManagerId.host +
          ":" + rdmaShuffleManagerId.port + " after " + Utils.getUsedTimeMs(startTime))
      }

      override def onFailure(e: Throwable): Unit = {
        logError("Failed to get block(s) from: " + rdmaShuffleManagerId + ", Exception: " + e)
        // TODO: startPartition may only be one of the partitions to report
        RdmaShuffleFetcherIterator.this.synchronized {
          resultsQueue.put(FailureFetchResult(startPartition, rdmaShuffleManagerId, e))
        }
        rdmaByteBufferManagedBuffers.foreach(_.release())
        // We skip curBytesInFlight since we expect one failure to fail the whole task
      }
    }

    try {
      rdmaChannel.rdmaReadInQueue(
        listener,
        rdmaRegisteredBuffer.getRegisteredAddress,
        rdmaRegisteredBuffer.getLkey,
        aggregatedPartitionGroup.locations.map(_.length).toArray,
        aggregatedPartitionGroup.locations.map(_.address).toArray,
        aggregatedPartitionGroup.locations.map(_.mKey).toArray)
    } catch {
      case e: Exception => listener.onFailure(e)
    }
  }

  private[this] def startAsyncRemoteFetches(): Unit = {
    val startRemotePartitionLocationFetch = System.currentTimeMillis()

    val futureSeq = for (partitionId <- startPartition until endPartition) yield {
      try {
        rdmaShuffleManager.fetchRemotePartitionLocations(shuffleId, partitionId)
      } catch {
        case e: Exception =>
          RdmaShuffleFetcherIterator.this.synchronized {
            resultsQueue.put(FailureMetadataFetchResult(
              new MetadataFetchFailedException(
                shuffleId,
                partitionId,
                "Failed to fetch remote partition locations for ShuffleId: " + shuffleId +
                  " PartitionId: " + partitionId + " from driver: " + e)))
          }
          null
      }
    }

    if (!futureSeq.contains(null)) { // if futureSeq contains null, abort everything
      val timeoutFutureSeq = wrapFuturesWithTimeout(futureSeq,
        rdmaShuffleConf.partitionLocationFetchTimeout)

      timeoutFutureSeq.onSuccess { case remotePartitionLocations =>
        logInfo("Fetching remote partition locations took " +
          (System.currentTimeMillis() - startRemotePartitionLocationFetch) + "ms")

        val groupedRemoteRdmaPartitionLocations = remotePartitionLocations.filter(_ != null)
          .flatten.filter(_.rdmaShuffleManagerId != localRdmaShuffleManagerId)
          .groupBy(_.rdmaShuffleManagerId)

        for ((rdmaShuffleManagerId, partitions) <- groupedRemoteRdmaPartitionLocations) {
          val aggregatedPartitionGroups = new ListBuffer[AggregatedPartitionGroup]

          var curAggregatedPartitionGroup = AggregatedPartitionGroup(0,
            new ListBuffer[RdmaBlockLocation])

          for (blockLocation <- partitions.map(_.rdmaBlockLocation)) {
            if (curAggregatedPartitionGroup.totalLength + blockLocation.length <=
                rdmaShuffleConf.shuffleReadBlockSize) {
              curAggregatedPartitionGroup.totalLength += blockLocation.length
            } else {
              if (curAggregatedPartitionGroup.totalLength > 0) {
                aggregatedPartitionGroups += curAggregatedPartitionGroup
              }
              curAggregatedPartitionGroup = AggregatedPartitionGroup(blockLocation.length,
                new ListBuffer[RdmaBlockLocation])
            }
            curAggregatedPartitionGroup.locations += blockLocation
          }

          if (curAggregatedPartitionGroup.totalLength > 0) {
            aggregatedPartitionGroups += curAggregatedPartitionGroup
          }

          for (aggregatedPartitionGroup <- aggregatedPartitionGroups) {
            numBlocksToFetch.addAndGet(aggregatedPartitionGroup.locations.length)

            if (curBytesInFlight.get() < maxBytesInFlight) {
              curBytesInFlight.addAndGet(aggregatedPartitionGroup.totalLength)
              Future { fetchBlocks(rdmaShuffleManagerId, aggregatedPartitionGroup) }
            } else {
              pendingFetchesQueue.add(PendingFetch(rdmaShuffleManagerId, aggregatedPartitionGroup))
            }
          }
        }

        insertDummyResult()
      }

      timeoutFutureSeq.onFailure { case error =>
        error match {
          case _: TimeoutException =>
            RdmaShuffleFetcherIterator.this.synchronized {
              resultsQueue.put(FailureMetadataFetchResult(
                new MetadataFetchFailedException(
                  shuffleId,
                  startPartition,
                  "Timed-out while fetching remote partition locations for ShuffleId: " +
                    shuffleId + " PartitionIds: " +
                    (startPartition until endPartition).mkString(", ") + " from driver, consider " +
                    "increasing the value of spark.shuffle.rdma.partitionLocationFetchTimeout " +
                    "(current value: " + rdmaShuffleConf.partitionLocationFetchTimeout + ")")))
            }
          case e: Exception =>
            RdmaShuffleFetcherIterator.this.synchronized {
              resultsQueue.put(FailureMetadataFetchResult(
                new MetadataFetchFailedException(
                  shuffleId,
                  startPartition,
                  "Failed to fetch remote partition locations for ShuffleId: " + shuffleId +
                    " PartitionIds: " + (startPartition until endPartition).mkString(", ") +
                    " from driver: " + e)))
            }
          case _ =>
            insertDummyResult()
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
          RdmaShuffleFetcherIterator.this.synchronized {
            resultsQueue.put(SuccessFetchResult(partitionId, localRdmaShuffleManagerId, in))
          }
          numBlocksToFetch.incrementAndGet()
        case _ =>
      }
    }
  }

  override def hasNext: Boolean = {
    numBlocksProcessed < numBlocksToFetch.get()
  }

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
      case SuccessFetchResult(_, rdmaShuffleManagerId, inputStream) if inputStream != null =>
        if (rdmaShuffleManagerId != localRdmaShuffleManagerId) {
          shuffleMetrics.incRemoteBytesRead(inputStream.available)
          shuffleMetrics.incRemoteBlocksFetched(1)
          curBytesInFlight.addAndGet(-inputStream.available)
        }
      case _ =>
    }

    // Start some pending remote fetches
    while (!pendingFetchesQueue.isEmpty && curBytesInFlight.get() < maxBytesInFlight) {
      pendingFetchesQueue.pollFirst()  match {
        case pendingFetch: PendingFetch =>
          curBytesInFlight.addAndGet(pendingFetch.aggregatedPartitionGroup.totalLength)
          Future {
            fetchBlocks(pendingFetch.rdmaShuffleManagerId, pendingFetch.aggregatedPartitionGroup)
          }
        case _ =>
      }
    }

    result match {
      case FailureMetadataFetchResult(e) => throw e

      case FailureFetchResult(partitionId, rdmaShuffleManagerId, e) =>
        // TODO: Throw exceptions for all of the mapIds?
        throw new FetchFailedException(
          rdmaShuffleManagerId.blockManagerId,
          shuffleId,
          0,
          partitionId,
          e)

      case SuccessFetchResult(_, _, inputStream) =>
        inputStream
    }
  }
}

// TODO: can we avoid this extra stream? just have release on the bytebufferbackedinputstream?
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
      rdmaShuffleManagerId: RdmaShuffleManagerId,
      inputStream: InputStream) extends FetchResult

  private[rdma] case class FailureFetchResult(
      partitionId: Int,
      rdmaShuffleManagerId: RdmaShuffleManagerId,
      e: Throwable) extends FetchResult

  private[rdma] case class FailureMetadataFetchResult(e: MetadataFetchFailedException)
      extends FetchResult
}
