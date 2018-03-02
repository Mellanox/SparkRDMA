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

import java.io.{File, InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.shuffle.rdma.{RdmaCompletionListener, _}
import org.apache.spark.shuffle.rdma.writer.RdmaShuffleData
import org.apache.spark.shuffle.sort._

class RdmaWrapperShuffleData(
    shuffleId: Int,
    numPartitions: Int,
    rdmaShuffleManager: RdmaShuffleManager) extends RdmaShuffleData {
  private val rdmaMappedFileByMapId = new ConcurrentHashMap[Int, RdmaMappedFile].asScala

  override def getInputStreams(partitionId: Int): Seq[InputStream] = {
    rdmaMappedFileByMapId.map(
      _._2.getByteBufferForPartition(partitionId)).filter(_ != null)
      .map(new ByteBufferBackedInputStream(_)).toSeq
  }

  override def dispose(): Unit = rdmaMappedFileByMapId.foreach(_._2.dispose())

  override def newShuffleWriter(): Unit = {}

  def getRdmaMappedFileForMapId(mapId: Int): RdmaMappedFile = rdmaMappedFileByMapId(mapId)

  override def removeDataByMap(mapId: Int): Unit = {
    rdmaMappedFileByMapId.remove(mapId).foreach(_.dispose())
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

      val rdmaFile = new RdmaMappedFile(dataFile,
        rdmaShuffleManager.rdmaShuffleConf.shuffleWriteBlockSize.toInt, lengths,
        rdmaShuffleManager.getRdmaBufferManager)
      // Overwrite and dispose of older file if already exists
      rdmaMappedFileByMapId.put(mapId, rdmaFile).foreach(_.dispose())
    }
  }
}

class RdmaWrapperShuffleWriter[K, V, C](
    rdmaShuffleBlockResolver: RdmaShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

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

  override def write(records: Iterator[Product2[K, V]]): Unit = writer.write(records)

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    val optMapStatus = writer.stop(success)
    val startTime = System.nanoTime()
    val rdmaShuffleManager = env.shuffleManager.asInstanceOf[RdmaShuffleManager]
    val rdmaShuffleConf = rdmaShuffleManager.rdmaShuffleConf
    if (success) {
      // Publish this map task's RdmaMapTaskOutput to the Driver
      val dep = handle.dependency
      val rdmaMapTaskOutput = rdmaShuffleBlockResolver.getRdmaShuffleData(dep.shuffleId).
        asInstanceOf[RdmaWrapperShuffleData].getRdmaMappedFileForMapId(mapId).getRdmaMapTaskOutput

      val rdmaChannel = rdmaShuffleManager.getRdmaChannelToDriver(mustRetry = true)
      val buffers = new RdmaPublishMapTaskOutputRpcMsg(
        rdmaShuffleManager.getLocalRdmaShuffleManagerId.blockManagerId,
        dep.shuffleId,
        mapId,
        dep.partitioner.numPartitions,
        0,
        dep.partitioner.numPartitions - 1,
        rdmaMapTaskOutput).toRdmaByteBufferManagedBuffers(
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
    }
    writeMetrics.incWriteTime(System.nanoTime - startTime)
    optMapStatus
  }
}
