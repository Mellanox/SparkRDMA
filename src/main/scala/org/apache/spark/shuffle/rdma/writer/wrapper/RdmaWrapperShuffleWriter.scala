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

package org.apache.spark.shuffle.rdma.writer.wrapper

import java.io.{File, IOException, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.shuffle.rdma._
import org.apache.spark.shuffle.rdma.writer.RdmaShuffleData
import org.apache.spark.shuffle.sort._

class RdmaWrapperShuffleData(
  shuffleId: Int,
  numPartitions: Int,
  rdmaShuffleManager: RdmaShuffleManager) extends RdmaShuffleData {

  private val rdmaMappedFileByMapId = new ConcurrentHashMap[Int, RdmaMappedFile]

  override def getInputStreams(partitionId: Int): Seq[InputStream] = {
    rdmaMappedFileByMapId.asScala.map {
      t: (Int, RdmaMappedFile) =>
        t._2.getByteBufferForPartition(partitionId) match {
          case buf: ByteBuffer => new ByteBufferBackedInputStream(buf)
          case _ => new InputStream {override def read(): Int = -1}
        }
    }.toSeq
  }

  override def dispose(): Unit = { rdmaMappedFileByMapId.asScala.foreach(_._2.dispose()) }

  override def newShuffleWriter(): Unit = {}

  def getRdmaMappedFileForMapId(mapId: Int): RdmaMappedFile = rdmaMappedFileByMapId.get(mapId)

  override def removeDataByMap(mapId: Int): Unit = {
    val file = rdmaMappedFileByMapId.remove(mapId)
    if (file != null) { file.dispose() }
  }

  override def writeIndexFileAndCommit(mapId: Int, lengths: Array[Long], dataTmp: File): Unit = {
    val dataFile = rdmaShuffleManager.shuffleBlockResolver.getDataFile(shuffleId, mapId)

    synchronized {
      if (dataFile.exists()) {
        dataFile.delete()
      }
      if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
        throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
      }
    }

    val rdmaFile = new RdmaMappedFile(dataFile, rdmaShuffleManager.getIbvPd,
      rdmaShuffleManager.rdmaShuffleConf.shuffleWriteBlockSize.toInt, lengths)

    val oldFile = rdmaMappedFileByMapId.put(mapId, rdmaFile)
    if (oldFile != null) { oldFile.dispose() }
  }
}

class RdmaWrapperShuffleWriter[K, V, C](
    rdmaShuffleBlockResolver: RdmaShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val env = SparkEnv.get
  private val writer = handle match {
    case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
      new UnsafeShuffleWriter(
        env.blockManager,
        rdmaShuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
        context.taskMemoryManager(),
        unsafeShuffleHandle,
        mapId,
        context,
        env.conf)
    case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
      new SortShuffleWriter(
        rdmaShuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
        other,
        mapId,
        context)
  }
  private var stopping = false

  override def write(records: Iterator[Product2[K, V]]): Unit = { writer.write(records) }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true

    val optMapStatus = writer.stop(success)

    if (success) {
      val rdmaShuffleManager = env.shuffleManager.asInstanceOf[RdmaShuffleManager]
      val localRdmaShuffleManagerId = rdmaShuffleManager.getLocalRdmaShuffleManagerId
      val dep = handle.dependency
      val rdmaMappedFile = rdmaShuffleBlockResolver.getRdmaShuffleData(dep.shuffleId).
        asInstanceOf[RdmaWrapperShuffleData].getRdmaMappedFileForMapId(mapId)

      val rdmaPartitionLocations = {
        for (partitionId <- 0 until dep.partitioner.numPartitions;
          rdmaBlockLocation = rdmaMappedFile.getRdmaBlockLocationForPartition(partitionId)
          if rdmaBlockLocation != null && rdmaBlockLocation.length > 0) yield {
          new RdmaPartitionLocation(localRdmaShuffleManagerId, partitionId, rdmaBlockLocation)
        }
      }

      // TODO: For increased safety, we can check if this is the latest when received in the driver
      val rdmaShuffleConf = rdmaShuffleManager.rdmaShuffleConf
      rdmaShuffleManager.publishPartitionLocations(
        rdmaShuffleConf.driverHost,
        rdmaShuffleConf.driverPort,
        dep.shuffleId,
        -1,
        rdmaPartitionLocations)
    }

    optMapStatus
  }
}
