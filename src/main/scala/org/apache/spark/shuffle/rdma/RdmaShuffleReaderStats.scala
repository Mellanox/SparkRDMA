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

import java.io.File
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManagerId

class RdmaRemoteFetchHistogram(numBuckets: Int, bucketSize: Int) {
  private val results = Array.fill(numBuckets + 1) { new AtomicInteger(0) }

  def addSample(sampleInMs: Int): Unit = {
    assume(sampleInMs >= 0)
    val bucket = sampleInMs / bucketSize
    if (bucket < numBuckets) {
      results(bucket).incrementAndGet
    } else { // sample is > max
      results(numBuckets).incrementAndGet
    }
  }

  def getString: String = {
    var str: String = ""
    for (i <- 0 until numBuckets) {
      str += ((i * bucketSize) + "-" + ((i + 1) * bucketSize) + "ms " + results(i) + ", ")
    }
    str += ((numBuckets * bucketSize) + "-...ms " + results(numBuckets))
    str
  }
}

class RdmaShuffleReaderStats(rdmaShuffleConf: RdmaShuffleConf) extends Logging {
  private val remoteFetchHistogramMap =
    new ConcurrentHashMap[BlockManagerId, RdmaRemoteFetchHistogram]()
  private val globalHistogram = new RdmaRemoteFetchHistogram(rdmaShuffleConf.fetchTimeNumBuckets,
    rdmaShuffleConf.fetchTimeBucketSizeInMs)

  def updateRemoteFetchHistogram(blockManagerId: BlockManagerId, fetchTimeInMs: Int): Unit = {
    var remoteFetchHistogram = remoteFetchHistogramMap.get(blockManagerId)
    if (remoteFetchHistogram == null) {
      remoteFetchHistogram = new RdmaRemoteFetchHistogram(rdmaShuffleConf.fetchTimeNumBuckets,
        rdmaShuffleConf.fetchTimeBucketSizeInMs)
      val res = remoteFetchHistogramMap.putIfAbsent(blockManagerId, remoteFetchHistogram)
      if (res != null) {
        remoteFetchHistogram = res
      }
    }
    remoteFetchHistogram.addSample(fetchTimeInMs)
    globalHistogram.addSample(fetchTimeInMs)
  }

  def printRemoteFetchHistogram(): Unit = {
    remoteFetchHistogramMap.asScala.foreach{ case (hostPort, histogram) =>
      logInfo("Fetch blocks from " + hostPort.toString + ": " + histogram.getString)
    }
    logInfo("Total fetches from all remote hostPorts: " + globalHistogram.getString)
  }
}

class OdpStats(rdmaShuffleConf: RdmaShuffleConf) extends Logging {
  private val statFsOdpFiles = Array("invalidations_faults_contentions", "num_invalidation_pages",
    "num_invalidations", "num_page_fault_pages", "num_page_faults", "num_prefetches_handled",
    "num_prefetch_pages")
  private val sysFsFolder = s"/sys/class/infiniband_verbs/uverbs${rdmaShuffleConf.rdmaDeviceNum}/"
  private val initialOdpStat = statFsOdpFiles.map(f =>
    Files.readFirstLine(new File(s"$sysFsFolder/$f"), Charset.defaultCharset()).toLong)

  def printODPStatistics(): Unit = {
    val odpStats = statFsOdpFiles.map(f =>
      Files.readFirstLine(new File(s"$sysFsFolder/$f"), Charset.defaultCharset()).toLong)
    val diff = odpStats.zip(initialOdpStat).map{case (v1, v2) => v1 - v2}
    diff.zip(statFsOdpFiles).foreach {
      case (value, fname) => logInfo(s"$fname: ${value}")
    }
  }
}
