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

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.rdma.ShuffleWriterMethod.ShuffleWriterMethod
import org.apache.spark.util.Utils

object ShuffleWriterMethod extends Enumeration {
  type ShuffleWriterMethod = Value
  val Wrapper, ChunkedPartitionAgg = Value
  def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)
}

class RdmaShuffleConf(conf: SparkConf) {
  private def getRdmaConfIntInRange(name: String, defaultValue: Int, min: Int, max: Int) = {
    conf.getInt(toRdmaConfKey(name), defaultValue)  match {
      case i if (min to max).contains(i) => i
      case _ => defaultValue
    }
  }

  private def getRdmaConfSizeAsBytesInRange(name: String, defaultValue: String, min: String,
      max: String) = conf.getSizeAsBytes(toRdmaConfKey(name), defaultValue) match {
        case i if i >= Utils.byteStringAsBytes(min) && i <= Utils.byteStringAsBytes(max) => i
        case _ => throw new IllegalArgumentException
      }

  private def getConfKey(name: String, defaultValue: String): String = conf.get(name, defaultValue)

  private def toRdmaConfKey(name: String) = "spark.shuffle.rdma." + name

  def getRdmaConfKey(name: String, defaultValue: String): String = getConfKey(toRdmaConfKey(name),
    defaultValue)

  def setDriverPort(value: String): Unit = conf.set(toRdmaConfKey("driverPort"), value)

  //
  // RDMA resource parameters
  //
  lazy val recvQueueDepth: Int = getRdmaConfIntInRange("recvQueueDepth", 2048, 0, 65535)
  lazy val sendQueueDepth: Int = getRdmaConfIntInRange("sendQueueDepth", 4096, 0, 65535)
  lazy val recvWrSize: Int = getRdmaConfSizeAsBytesInRange("recvWrSize", "4k", "2k", "1m").toInt

  //
  // CPU Affinity Settings
  //
  lazy val cpuList: String = getRdmaConfKey("cpuList", "")

  //
  // Shuffle writer configuration
  //
  lazy val shuffleWriterMethod: ShuffleWriterMethod = ShuffleWriterMethod.withNameOpt(
    getRdmaConfKey("shuffleWriterMethod", "Wrapper")).getOrElse(ShuffleWriterMethod.Wrapper)
  lazy val shuffleWriteChunkSize: Long = getRdmaConfSizeAsBytesInRange(
    "shuffleWriteChunkSize", "128k", "4k", "128m")
  lazy val shuffleWriteFlushSize: Long = getRdmaConfSizeAsBytesInRange(
    "shuffleWriteFlushSize", "256k", "4k", "128m")
  lazy val shuffleWriteBlockSize: Long = getRdmaConfSizeAsBytesInRange(
    "shuffleWriteBlockSize", "8m", "4k", "512m")
  lazy val shuffleWriteMaxInMemoryStoragePerExecutor: Long = getRdmaConfSizeAsBytesInRange(
    "shuffleWriteMaxInMemoryStoragePerExecutor", "25g", "0", "10t")
  // TODO: Limit to machine memory

  //
  // Shuffle reader configuration
  //
  lazy val shuffleReadBlockSize: Long = getRdmaConfSizeAsBytesInRange(
    "shuffleReadBlockSize", "8m", "128k", "512m")
  lazy val maxBytesInFlight: Long = getRdmaConfSizeAsBytesInRange(
    "maxBytesInFlight", "128m", "48m", "100g")
  lazy val maxAggBlock: Long = getRdmaConfSizeAsBytesInRange("maxAggBlock", "2m", "2m", "1g")
  lazy val maxAggPrealloc: Long = getRdmaConfSizeAsBytesInRange("maxAggPrealloc", "0", "0", "10g")

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
}
