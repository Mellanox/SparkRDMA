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

import java.io._
import java.nio.charset.Charset

case class HostPort(var host: String, var port: Int)
case class RdmaBlockLocation(var address: Long, var length: Int, var mKey: Int)

class RdmaPartitionLocation(
    var hostPort: HostPort,
    var partitionId : Int,
    var rdmaBlockLocation: RdmaBlockLocation) {

  private var hostnameInUtf: Array[Byte] = _
  private def this() = this(HostPort(null, 0), 0, null)  // For deserialization only

  def serializedLength: Int = {
    if (hostnameInUtf == null) {
      hostnameInUtf = hostPort.host.getBytes(Charset.forName("UTF-8"))
    }
    2 + hostnameInUtf.length + 4 + 4 + 8 + 4 + 4
  }

  def write(out: DataOutputStream) {
    if (hostnameInUtf == null) {
      hostnameInUtf = hostPort.host.getBytes(Charset.forName("UTF-8"))
    }
    out.writeShort(hostnameInUtf.length)
    out.write(hostnameInUtf)
    hostnameInUtf = null
    out.writeInt(hostPort.port)
    out.writeInt(partitionId)
    out.writeLong(rdmaBlockLocation.address)
    out.writeInt(rdmaBlockLocation.length)
    out.writeInt(rdmaBlockLocation.mKey)
  }

  def read(in: DataInputStream) {
    val hostLength = in.readShort()
    hostnameInUtf = new Array[Byte](hostLength)
    in.read(hostnameInUtf, 0, hostLength)
    hostPort.host = new String(hostnameInUtf, "UTF-8")
    hostnameInUtf = null
    hostPort.port = in.readInt()
    partitionId = in.readInt()
    rdmaBlockLocation = RdmaBlockLocation(in.readLong(), in.readInt(), in.readInt())
  }
}

object RdmaPartitionLocation {
  def apply(in: DataInputStream): RdmaPartitionLocation = {
    val obj = new RdmaPartitionLocation()
    obj.read(in)
    obj
  }
}
