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

import org.apache.spark.storage.BlockManagerId

case class RdmaBlockLocation(var address: Long, var length: Int, var mKey: Int)

class RdmaPartitionLocation(
    var rdmaShuffleManagerId: RdmaShuffleManagerId,
    var partitionId: Int,
    var rdmaBlockLocation: RdmaBlockLocation) {

  private def this() = this(null, 0, null)  // For deserialization only

  def serializedLength: Int = {
    rdmaShuffleManagerId.serializedLength + 4 + 8 + 4 + 4
  }

  def write(out: DataOutputStream) {
    rdmaShuffleManagerId.write(out)
    out.writeInt(partitionId)
    out.writeLong(rdmaBlockLocation.address)
    out.writeInt(rdmaBlockLocation.length)
    out.writeInt(rdmaBlockLocation.mKey)
  }

  def read(in: DataInputStream) {
    rdmaShuffleManagerId = RdmaShuffleManagerId(in)
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

class RdmaShuffleManagerId(var host: String, var port: Int, var blockManagerId: BlockManagerId) {
  private var hostnameInUtf: Array[Byte] = _
  private var executorIdInUtf: Array[Byte] = _
  private var blockManagerHostNameInUtf: Array[Byte] = _

  // For deserialization only
  private def this() = this(null, 0, null)

  override def toString: String = s"RdmaShuffleManagerId($host, $port, $blockManagerId)"

  override def hashCode: Int = blockManagerId.hashCode

  def serializedLength: Int = {
    if (hostnameInUtf == null) {
      hostnameInUtf = host.getBytes(Charset.forName("UTF-8"))
    }
    if (executorIdInUtf == null) {
      executorIdInUtf = blockManagerId.executorId.getBytes(Charset.forName("UTF-8"))
    }
    if (blockManagerHostNameInUtf == null) {
      blockManagerHostNameInUtf = blockManagerId.host.getBytes(Charset.forName("UTF-8"))
    }
    2 + hostnameInUtf.length + 4 + 2 + executorIdInUtf.length + 2 +
      blockManagerHostNameInUtf.length + 4
  }

  def write(out: DataOutputStream) {
    if (hostnameInUtf == null) {
      hostnameInUtf = host.getBytes(Charset.forName("UTF-8"))
    }
    out.writeShort(hostnameInUtf.length)
    out.write(hostnameInUtf)

    out.writeInt(port)

    if (executorIdInUtf == null) {
      executorIdInUtf = blockManagerId.executorId.getBytes(Charset.forName("UTF-8"))
    }
    out.writeShort(executorIdInUtf.length)
    out.write(executorIdInUtf)

    if (blockManagerHostNameInUtf == null) {
      blockManagerHostNameInUtf = blockManagerId.host.getBytes(Charset.forName("UTF-8"))
    }
    out.writeShort(blockManagerHostNameInUtf.length)
    out.write(blockManagerHostNameInUtf)

    out.writeInt(blockManagerId.port)
  }

  def read(in: DataInputStream) {
    var length = in.readShort()
    hostnameInUtf = new Array[Byte](length)
    in.read(hostnameInUtf, 0, length)
    host = new String(hostnameInUtf, "UTF-8")

    port = in.readInt()

    length = in.readShort()
    executorIdInUtf = new Array[Byte](length)
    in.read(executorIdInUtf, 0, length)
    val executorId = new String(executorIdInUtf, "UTF-8")

    length = in.readShort()
    blockManagerHostNameInUtf = new Array[Byte](length)
    in.read(blockManagerHostNameInUtf, 0, length)
    val blockManagerHost = new String(blockManagerHostNameInUtf, "UTF-8")

    val blockManagerPort = in.readInt()
    blockManagerId = BlockManagerId(executorId, blockManagerHost, blockManagerPort)
  }

  override def equals(that: Any): Boolean = that match {
    case id: RdmaShuffleManagerId =>
      port == id.port && host == id.host && blockManagerId == id.blockManagerId
    case _ =>
      false
  }
}

object RdmaShuffleManagerId {
  def apply(in: DataInputStream): RdmaShuffleManagerId = {
    val obj = new RdmaShuffleManagerId()
    obj.read(in)
    obj
  }
}
