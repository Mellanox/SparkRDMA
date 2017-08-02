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

package org.apache.spark.shuffle.rdma.writer.chunkedpartitionagg

import java.io.{File, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.shuffle.rdma._
import org.apache.spark.shuffle.rdma.writer.RdmaShuffleData
import org.apache.spark.storage.ShuffleBlockId

class RdmaChunkedPartitionAggShuffleData(shuffleId: Int, numPartitions: Int,
    rdmaShuffleManager: RdmaShuffleManager) extends RdmaShuffleData {
  private val writers = Array.fill(numPartitions) {
    new RdmaShufflePartitionWriter(rdmaShuffleManager)
  }
  private val activeShuffleWriters = new AtomicInteger(0)

  def write(bufs: Array[ByteBuffer], totalLength: Long, shuffleId: Int, partitionId: Int): Unit = {
    writers(partitionId).write(bufs, totalLength)
  }

  def commitMapOutput(mapOutputByteBuffers : Array[RdmaChunkedByteBuffer], shuffleId: Int): Long = {
    val startTime = System.nanoTime()
    for ((buf, partitionId) <- mapOutputByteBuffers.zipWithIndex) {
      write(buf.getChunks(), buf.length, shuffleId, partitionId)
    }

    val remainingWriters = activeShuffleWriters.decrementAndGet()
    if (remainingWriters == 0) this.synchronized {
      // TODO: we can use a map instead of foreach, and flatten
      val rdmaPartitionLocations = new ArrayBuffer[RdmaPartitionLocation]
      writers.zipWithIndex.foreach {
        case (writer: RdmaShufflePartitionWriter, partitionId: Int) =>
          for (location <- writer.getLocations) {
            rdmaPartitionLocations += new RdmaPartitionLocation(
              rdmaShuffleManager.getLocalHostPort,
              partitionId,
              location)
          }
      }

      rdmaShuffleManager.publishPartitionLocations(
        rdmaShuffleManager.rdmaShuffleConf.driverHost,
        rdmaShuffleManager.rdmaShuffleConf.driverPort,
        shuffleId,
        rdmaPartitionLocations)
    }

    System.nanoTime() - startTime
  }

  override def getInputStreams(partitionId: Int): Seq[InputStream] = {
    writers(partitionId).getInputStreams
  }

  override def dispose(): Unit = {
    writers.foreach(_.dispose())
  }

  override def newShuffleWriter(): Unit = { activeShuffleWriters.getAndIncrement() }

  override def removeDataByMap(mapId: Int): Unit = {}

  override def writeIndexFileAndCommit(mapId: Int, lengths: Array[Long], dataTmp: File): Unit = {}
}

private[spark] class RdmaChunkedPartitionAggShuffleWriter[K, V](
  rdmaShuffleBlockResolver: RdmaShuffleBlockResolver,
  handle: BaseShuffleHandle[K, V, _],
  mapId: Int,
  context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val blockManager = SparkEnv.get.blockManager
  private var stopping = false
  private val serializerManager = SparkEnv.get.serializerManager
  private val serializerInstance = dep.serializer.newInstance()
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics
  private val rdmaShuffleManager = SparkEnv.get.shuffleManager.asInstanceOf[RdmaShuffleManager]
  private val rdmaShuffleConf = rdmaShuffleManager.rdmaShuffleConf
  private val shuffleWriteChunkSize = rdmaShuffleConf.shuffleWriteChunkSize.asInstanceOf[Int]
  private val shuffleWriteFlushSize = rdmaShuffleConf.shuffleWriteFlushSize.asInstanceOf[Int]
  private val rdmaChunkedPartitionAggregationShuffleData =
    rdmaShuffleBlockResolver.getRdmaShuffleData(dep.shuffleId) match {
      case a: RdmaChunkedPartitionAggShuffleData => a
      case _ => throw new IllegalStateException("Wrong RdmaShuffleData type")
    }

  // TODO: combine into a single array
  private val outputStreams = Array.fill(dep.partitioner.numPartitions) {
    new RdmaChunkedByteBufferOutputStream(shuffleWriteChunkSize)
  }

  private val dummyShuffleBlockId = ShuffleBlockId(0, 0, 0)
  private val compressedStreams = Array.tabulate(dep.partitioner.numPartitions) {
    partitionId => serializerManager.wrapForCompression(
      dummyShuffleBlockId, outputStreams(partitionId))
  }

  private val serializationStreams = Array.tabulate(dep.partitioner.numPartitions) {
    partitionId => serializerInstance.serializeStream(compressedStreams(partitionId))
  }

  private val serializationStreamsNumRecords = Array.fill(dep.partitioner.numPartitions) {
    new Integer(0)
  }

  private var recordSizeAvgAndCount = (Long.MaxValue, 0)

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    var numRecordsWritten = 0
    var numBytesWritten = 0L
    var writeTime = 0L

    val iter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      dep.aggregator.get.combineValuesByKey(records, context)
    } else {
      records
    }

    for (kv <- iter) {
      // TODO: can we aggregate KVs by part id and produce new iterators, then writeAll() at once?
      // TODO: if K, V types are constant size, then we can write directly
      val partitionId = dep.partitioner.getPartition(kv._1)
      serializationStreams(partitionId).writeKey[Any](kv._1)
      serializationStreams(partitionId).writeValue[Any](kv._2)
      serializationStreamsNumRecords(partitionId) += 1

      if (serializationStreamsNumRecords(partitionId) * recordSizeAvgAndCount._1 >=
        shuffleWriteFlushSize) {

        serializationStreams(partitionId).close()
        recordSizeAvgAndCount = {
          if (recordSizeAvgAndCount._1 != Long.MaxValue) {
            ((recordSizeAvgAndCount._1 * recordSizeAvgAndCount._2 +
              outputStreams(partitionId).size) / (recordSizeAvgAndCount._2 +
              serializationStreamsNumRecords(partitionId)),
              recordSizeAvgAndCount._2 + serializationStreamsNumRecords(partitionId))
          } else {
            (outputStreams(partitionId).size / serializationStreamsNumRecords(partitionId),
              serializationStreamsNumRecords(partitionId))
          }
        }
        serializationStreamsNumRecords(partitionId) = 0

        val startTime = System.nanoTime()

        val rdmaChunkedByteBuffer = outputStreams(partitionId).toRdmaChunkedByteBuffer
        rdmaChunkedPartitionAggregationShuffleData.write(
          rdmaChunkedByteBuffer.getChunks(),
          rdmaChunkedByteBuffer.length,
          dep.shuffleId,
          partitionId)
        numBytesWritten += outputStreams(partitionId).size

        // Recreate new streams from the recycled chunks
        outputStreams(partitionId) = new RdmaChunkedByteBufferOutputStream(shuffleWriteChunkSize,
          rdmaChunkedByteBuffer.getRdmaBufferChunks())
        // TODO: do we really have to create new streams from scratch? not really, finish() on the
        // compressed stream is enough, but not availiable on all compressors
        compressedStreams(partitionId) = serializerManager.wrapForCompression(
          dummyShuffleBlockId, outputStreams(partitionId))
        serializationStreams(partitionId) = serializerInstance.serializeStream(
          compressedStreams(partitionId))

        writeTime += System.nanoTime() - startTime
      }

      numRecordsWritten += 1
    }

    writeMetrics.incRecordsWritten(numRecordsWritten)
    writeMetrics.incBytesWritten(numBytesWritten)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        serializationStreams.foreach(_.close())

        val lengths: Array[Long] = outputStreams.map { outputStream => outputStream.size }
        lengths.foreach(writeMetrics.incBytesWritten)

        val arrBuffers = outputStreams.map { outputStream => outputStream.toRdmaChunkedByteBuffer }
        val writeTime = rdmaChunkedPartitionAggregationShuffleData.commitMapOutput(arrBuffers,
          dep.shuffleId)
        writeMetrics.incWriteTime(writeTime)

        arrBuffers.foreach(_.dispose())
        // TODO: these are wrong lengths - need accumulate
        Some(MapStatus(blockManager.shuffleServerId, lengths))
      } else {
        None
      }
    } finally {
      // TODO: ???
    }
  }
}