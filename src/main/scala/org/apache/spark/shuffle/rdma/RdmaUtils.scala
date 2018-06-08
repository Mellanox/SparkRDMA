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
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.sort.SerializedShuffleHandle
import org.apache.spark.storage.BlockManagerId

case class RdmaBlockLocation(var address: Long, var length: Int, var mKey: Int) {
  require(length >= 0, s"Block length must be >=0, but $length is set")
}

class SerializableBlockManagerId private (executorId__ : String, host__ : String, port__ : Int) {
  private val blockManagerId = BlockManagerId(executorId__, host__, port__)
  private var executorIdInUtf: Array[Byte] = _
  private var blockManagerHostNameInUtf: Array[Byte] = _

  def this(blockManagerId: BlockManagerId) {
    this(blockManagerId.executorId, blockManagerId.host, blockManagerId.port)
  }

  def toBlockManagerId: BlockManagerId = blockManagerId

  def serializedLength: Int = {
    if (executorIdInUtf == null) {
      executorIdInUtf = blockManagerId.executorId.getBytes(Charset.forName("UTF-8"))
    }
    if (blockManagerHostNameInUtf == null) {
      blockManagerHostNameInUtf = blockManagerId.host.getBytes(Charset.forName("UTF-8"))
    }
    2 + executorIdInUtf.length + 2 + blockManagerHostNameInUtf.length + 4
  }

  def write(out: DataOutputStream) {
    out.writeShort(executorIdInUtf.length)
    out.write(executorIdInUtf)
    out.writeShort(blockManagerHostNameInUtf.length)
    out.write(blockManagerHostNameInUtf)
    out.writeInt(blockManagerId.port)
  }
}

object SerializableBlockManagerId {
  def apply(in: DataInputStream): SerializableBlockManagerId = {
    val executorIdInUtf = new Array[Byte](in.readShort())
    in.read(executorIdInUtf)
    val blockManagerHostNameInUtf = new Array[Byte](in.readShort())
    in.read(blockManagerHostNameInUtf)
    new SerializableBlockManagerId(new String(executorIdInUtf, "UTF-8"),
      new String(blockManagerHostNameInUtf, "UTF-8"), in.readInt())
  }
}

class RdmaShuffleManagerId private (
    var host: String,
    var port: Int,
    var blockManagerId: BlockManagerId) {
  private var hostnameInUtf: Array[Byte] = _
  private var serializableBlockManagerId = if (blockManagerId != null) {
    new SerializableBlockManagerId(blockManagerId)
  } else {
    null
  }

  // For deserialization only
  private def this() = this(null, 0, null)

  override def toString: String = s"RdmaShuffleManagerId($host, $port, $blockManagerId)"

  override def hashCode: Int = blockManagerId.hashCode

  def serializedLength: Int = {
    if (hostnameInUtf == null) {
      hostnameInUtf = host.getBytes(Charset.forName("UTF-8"))
    }
    2 + hostnameInUtf.length + 4 + serializableBlockManagerId.serializedLength
  }

  def write(out: DataOutputStream) {
    if (hostnameInUtf == null) {
      hostnameInUtf = host.getBytes(Charset.forName("UTF-8"))
    }
    out.writeShort(hostnameInUtf.length)
    out.write(hostnameInUtf)
    out.writeInt(port)
    serializableBlockManagerId.write(out)
  }

  private def read(in: DataInputStream) {
    hostnameInUtf = new Array[Byte](in.readShort())
    in.read(hostnameInUtf)
    host = new String(hostnameInUtf, "UTF-8")
    port = in.readInt()
    serializableBlockManagerId = SerializableBlockManagerId(in)
    blockManagerId = serializableBlockManagerId.toBlockManagerId
  }

  override def equals(that: Any): Boolean = that match {
    case id: RdmaShuffleManagerId =>
      port == id.port && host == id.host && blockManagerId == id.blockManagerId
    case _ =>
      false
  }
}

object RdmaShuffleManagerId {
  def apply(host: String, port: Int, blockManagerId: BlockManagerId): RdmaShuffleManagerId =
    getCachedRdmaShuffleManagerId(new RdmaShuffleManagerId(host, port, blockManagerId))

  def apply(in: DataInputStream): RdmaShuffleManagerId = {
    val obj = new RdmaShuffleManagerId()
    obj.read(in)
    getCachedRdmaShuffleManagerId(obj)
  }

  val rdmaShuffleManagerIdIdCache =
    new ConcurrentHashMap[RdmaShuffleManagerId, RdmaShuffleManagerId]()

  def getCachedRdmaShuffleManagerId(id: RdmaShuffleManagerId): RdmaShuffleManagerId = {
    rdmaShuffleManagerIdIdCache.putIfAbsent(id, id)
    rdmaShuffleManagerIdIdCache.get(id)
  }
}

class RdmaBaseShuffleHandle[K, V, C](val driverTableAddress: Long,
    val driverTableLength: Int,
    val driverTableRKey: Int,
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C])
    extends BaseShuffleHandle[K, V, C](shuffleId, numMaps, dependency)

class RdmaSerializedShuffleHandle[K, V](val driverTableAddress: Long,
    val driverTableLength: Int,
    val driverTableRKey: Int,
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, V])
    extends SerializedShuffleHandle[K, V](shuffleId, numMaps, dependency)
