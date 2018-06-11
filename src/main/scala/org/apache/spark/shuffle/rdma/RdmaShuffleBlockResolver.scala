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

import java.io.{File, InputStream}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver}
import org.apache.spark.shuffle.rdma.writer.wrapper.RdmaWrapperShuffleData
import org.apache.spark.storage.ShuffleBlockId

class RdmaShuffleBlockResolver(rdmaShuffleManager: RdmaShuffleManager)
    extends IndexShuffleBlockResolver(rdmaShuffleManager.conf) with Logging {
  private val rdmaShuffleDataMap = new ConcurrentHashMap[Int, RdmaWrapperShuffleData]

  def newShuffleWriter[K, V](baseShuffleHandle: BaseShuffleHandle[K, V, _]): Unit = synchronized {
    val shuffleId = baseShuffleHandle.shuffleId
    val numPartitions = baseShuffleHandle.dependency.partitioner.numPartitions

    var rdmaShuffleData = rdmaShuffleDataMap.get(shuffleId)
    if (rdmaShuffleData == null) {
      rdmaShuffleData = new RdmaWrapperShuffleData(shuffleId, numPartitions, rdmaShuffleManager)
      rdmaShuffleDataMap.put(shuffleId, rdmaShuffleData)
    }
    rdmaShuffleData.newShuffleWriter()
  }

  def removeShuffle(shuffleId: Int): Unit = {
    rdmaShuffleDataMap.remove(shuffleId) match {
      case r: RdmaWrapperShuffleData => r.dispose()
      case null =>
    }
  }

  def getRdmaShuffleData(shuffleId: ShuffleId): RdmaWrapperShuffleData =
    rdmaShuffleDataMap.get(shuffleId)

  override def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    getRdmaShuffleData(shuffleId).removeDataByMap(mapId)
  }

  override def writeIndexFileAndCommit(
    shuffleId: Int,
    mapId: Int,
    lengths: Array[Long],
    dataTmp: File): Unit = {
    getRdmaShuffleData(shuffleId).writeIndexFileAndCommit(mapId, lengths, dataTmp)
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    throw new UnsupportedOperationException
  }

  override def stop(): Unit = {}

  def getLocalRdmaPartition(shuffleId: Int, partitionId : Int) : Seq[InputStream] = {
    rdmaShuffleDataMap.get(shuffleId) match {
      case r: RdmaWrapperShuffleData => r.getInputStreams(partitionId)
      case null => Seq.empty
    }
  }
}
