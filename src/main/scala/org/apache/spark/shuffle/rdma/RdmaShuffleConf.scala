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

import com.ibm.disni.rdma.verbs.IbvContext

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Utils, VersionUtils}

object SparkVersionSupport {
  val majorVersion = VersionUtils.majorVersion(SPARK_VERSION)
  if (majorVersion != 2) {
    throw new IllegalArgumentException("SparkRDMA only supports Spark versions 2.x")
  }
  val minorVersion = VersionUtils.minorVersion(SPARK_VERSION)
}

class RdmaShuffleConf(conf: SparkConf) extends Logging{

  private def getRdmaConfIntInRange(name: String, defaultValue: Int, min: Int, max: Int) = {
    conf.getInt(toRdmaConfKey(name), defaultValue)  match {
      case i if (min to max).contains(i) => i
      case _ => defaultValue
    }
  }

  private def getRdmaConfSizeAsBytesInRange(name: String, defaultValue: String, min: String,
      max: String) = conf.getSizeAsBytes(toRdmaConfKey(name), defaultValue) match {
        case i if i >= Utils.byteStringAsBytes(min) && i <= Utils.byteStringAsBytes(max) => i
        case _ => Utils.byteStringAsBytes(defaultValue)
      }

  def getConfKey(name: String, defaultValue: String): String = conf.get(name, defaultValue)

  private def toRdmaConfKey(name: String) = "spark.shuffle.rdma." + name

  def getRdmaConfKey(name: String, defaultValue: String): String = getConfKey(toRdmaConfKey(name),
    defaultValue)

  def setDriverPort(value: String): Unit = conf.set(toRdmaConfKey("driverPort"), value)

  //
  // RDMA resource parameters
  //
  lazy val recvQueueDepth: Int = getRdmaConfIntInRange("recvQueueDepth", 256, 256, 65535)
  lazy val sendQueueDepth: Int = getRdmaConfIntInRange("sendQueueDepth", 4096, 256, 65535)
  lazy val recvWrSize: Int = getRdmaConfSizeAsBytesInRange("recvWrSize", "4k", "2k", "1m").toInt
  lazy val swFlowControl: Boolean = conf.getBoolean(toRdmaConfKey("swFlowControl"), true)
  lazy val maxBufferAllocationSize: Long = getRdmaConfSizeAsBytesInRange(
      "maxBufferAllocationSize", "10g", "0", "10t")
  // Only required to collect ODP stats from sysfs
  lazy val rdmaDeviceNum: Int = conf.getInt(toRdmaConfKey("device.num"), 0)

  def useOdp(context: IbvContext): Boolean = {
    conf.getBoolean(toRdmaConfKey("useOdp"), false) && {
      val rcOdpCaps = context.queryOdpSupport()
      val ret = (rcOdpCaps != -1) &&
        ((rcOdpCaps & IbvContext.IBV_ODP_SUPPORT_WRITE) != 0) &&
        ((rcOdpCaps & IbvContext.IBV_ODP_SUPPORT_READ) != 0)
      if (!ret) {
        logWarning("ODP (On Demand Paging) is not supported for this device. " +
          "Please refer to the SparkRDMA wiki for more information: " +
          "https://github.com/Mellanox/SparkRDMA/wiki/Configuration-Properties")
      } else {
        logInfo("Using ODP (On Demand Paging) memory prefetch")
      }
      ret
    }
  }
  //
  // CPU Affinity Settings
  //
  lazy val cpuList: String = getRdmaConfKey("cpuList", "")

  //
  // Shuffle writer configuration
  //
  lazy val shuffleWriteBlockSize: Long = getRdmaConfSizeAsBytesInRange(
    "shuffleWriteBlockSize", "8m", "4k", "512m")

  //
  // Shuffle reader configuration
  //
  lazy val shuffleReadBlockSize: Long = getRdmaConfSizeAsBytesInRange(
    "shuffleReadBlockSize", "256k", "0", "512m")
  lazy val maxBytesInFlight: Long = getRdmaConfSizeAsBytesInRange(
    "maxBytesInFlight", "48m", "128k", "100g")

  // Comma separated list of buffer size : buffer count pairs. E.g. 4k:1000,16k:500
  lazy val preAllocateBuffers: Map[Int, Int] = {
    val bufferMap = getRdmaConfKey("preAllocateBuffers", "")
      .split(",").withFilter(s => !s.isEmpty)
      .map(entry => entry.split(":") match {
      case Array(bufferSize, bufferCount) =>
        (Utils.byteStringAsBytes(bufferSize.trim).toInt, bufferCount.toInt)
    }).toMap
    if (bufferMap.keys.sum >= maxBufferAllocationSize) {
      throw new Exception("Total pre allocation buffer size >= " +
        "spark.shuffle.rdma.maxBufferAllocationSize")
    }
    bufferMap
  }

  // Remote fetch block statistics
  lazy val collectShuffleReaderStats: Boolean = conf.getBoolean(
    toRdmaConfKey("collectShuffleReaderStats"),
    defaultValue = false)
  lazy val partitionLocationFetchTimeout: Int = getRdmaConfIntInRange(
    "partitionLocationFetchTimeout", 120000, 1000, Integer.MAX_VALUE)
  lazy val fetchTimeBucketSizeInMs: Int = getRdmaConfIntInRange("fetchTimeBucketSizeInMs", 300, 5,
    60000)
  lazy val fetchTimeNumBuckets: Int = getRdmaConfIntInRange("fetchTimeNumBuckets", 5, 2, 100)
  // ODP statistics
  lazy val collectOdpStats: Boolean = conf.getBoolean(toRdmaConfKey("collectOdpStats"), true)
  //
  // Addressing and connection configuration
  //
  lazy val driverHost: String = conf.get("spark.driver.host")
  lazy val driverPort: Int = getRdmaConfIntInRange("driverPort", 0, 1025, 65535)
  lazy val executorPort: Int = getRdmaConfIntInRange("executorPort", 0, 1025, 65535)
  lazy val portMaxRetries: Int = conf.getInt("spark.port.maxRetries", 16)
  lazy val rdmaCmEventTimeout: Int = getRdmaConfIntInRange("rdmaCmEventTimeout", 20000, -1, 60000)
  lazy val teardownListenTimeout: Int = getRdmaConfIntInRange("teardownListenTimeout", 50, -1,
    60000)
  lazy val resolvePathTimeout: Int = getRdmaConfIntInRange("resolvePathTimeout", 2000, -1, 60000)
  lazy val maxConnectionAttempts: Int = getRdmaConfIntInRange("maxConnectionAttempts", 5, 1, 100)
}
