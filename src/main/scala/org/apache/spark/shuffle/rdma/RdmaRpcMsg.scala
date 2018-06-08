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

import java.io.{DataInputStream, DataOutputStream, EOFException}
import java.nio.ByteBuffer

import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rdma.RdmaRpcMsgType.RdmaRpcMsgType
import org.apache.spark.storage.BlockManagerId


object RdmaRpcMsgType extends Enumeration {
  type RdmaRpcMsgType = Value
  val RdmaShuffleManagerHello, AnnounceRdmaShuffleManagers, PublishMapTaskOutput, FetchMapStatus,
    FetchMapStatusResponse = Value
}

trait RdmaRpcMsg {
  protected def msgType: RdmaRpcMsgType
  protected def getLengthInSegments(segmentSize: Int): Array[Int]
  protected def read(dataIn: DataInputStream): Unit
  protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit

  private final val overhead: Int = 4 + 4 // 4 + 4 for msg length and type

  def toRdmaByteBufferManagedBuffers(allocator: Int => RdmaByteBufferManagedBuffer,
      maxSegmentSize: Int): Array[RdmaByteBufferManagedBuffer] = {
    val arrSegmentLengths = getLengthInSegments(maxSegmentSize - overhead)
    val bufs = Array.fill(arrSegmentLengths.length) { allocator(maxSegmentSize) }

    val outs = for ((buf, bufferIndex) <- bufs.zipWithIndex) yield {
      val out = new DataOutputStream(buf.createOutputStream())
      out.writeInt(overhead + arrSegmentLengths(bufferIndex))
      out.writeInt(msgType.id)
      (out, arrSegmentLengths(bufferIndex))
    }

    writeSegments(outs.iterator)
    outs.foreach(_._1.close())

    bufs
  }
}

object RdmaRpcMsg extends Logging {
  private val logger = LoggerFactory.getLogger(classOf[RdmaRpcMsg])

  def apply(buf: ByteBuffer): RdmaRpcMsg = {
    val in = new DataInputStream(new ByteBufferBackedInputStream(buf))
    val msgLength = in.readInt()
    buf.limit(msgLength)

    RdmaRpcMsgType(in.readInt()) match {
      case RdmaRpcMsgType.RdmaShuffleManagerHello =>
        RdmaShuffleManagerHelloRpcMsg(in)
      case RdmaRpcMsgType.AnnounceRdmaShuffleManagers =>
        RdmaAnnounceRdmaShuffleManagersRpcMsg(in)
      case RdmaRpcMsgType.PublishMapTaskOutput =>
        RdmaPublishMapTaskOutputRpcMsg(in)
      case RdmaRpcMsgType.FetchMapStatus =>
        RdmaFetchMapStatusRpcMsg(in)
      case RdmaRpcMsgType.FetchMapStatusResponse =>
        RdmaFetchMapStatusResponseRpcMsg(in)
      case _ =>
        logger.warn("Received an unidentified RPC")
        null
    }
  }
}

class RdmaShuffleManagerHelloRpcMsg(var rdmaShuffleManagerId: RdmaShuffleManagerId)
    extends RdmaRpcMsg {
  private def this() = this(null)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.RdmaShuffleManagerHello

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    val serializedLength = rdmaShuffleManagerId.serializedLength
    require(serializedLength <= segmentSize, "RdmaBuffer RPC segment size is too small")

    Array.fill(1) { serializedLength }
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    val out = outs.next()._1
    rdmaShuffleManagerId.write(out)
  }

  override protected def read(in: DataInputStream): Unit = {
    rdmaShuffleManagerId = RdmaShuffleManagerId(in)
  }
}

object RdmaShuffleManagerHelloRpcMsg {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaShuffleManagerHelloRpcMsg()
    obj.read(in)
    obj
  }
}

class RdmaAnnounceRdmaShuffleManagersRpcMsg(var rdmaShuffleManagerIds: Seq[RdmaShuffleManagerId])
    extends RdmaRpcMsg {
  private def this() = this(null)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.AnnounceRdmaShuffleManagers

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    var segmentSizes = new ArrayBuffer[Int]
    for (rdmaShuffleManagerId <- rdmaShuffleManagerIds) {
      val serializedLength = rdmaShuffleManagerId.serializedLength

      if (segmentSizes.nonEmpty && (segmentSizes.last + serializedLength <= segmentSize)) {
        segmentSizes.update(segmentSizes.length - 1, segmentSizes.last + serializedLength)
      } else {
        segmentSizes += serializedLength
      }
    }

    segmentSizes.toArray
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    var curOut: (DataOutputStream, Int) = null
    var curSegmentLength = 0

    def nextOut() {
      curOut = outs.next()
      curSegmentLength = 0
    }

    nextOut()
    for (rdmaShuffleManagerId <- rdmaShuffleManagerIds) {
      val serializedLength = rdmaShuffleManagerId.serializedLength

      if (curSegmentLength + serializedLength > curOut._2) {
        nextOut()
      }
      curSegmentLength += serializedLength
      rdmaShuffleManagerId.write(curOut._1)
    }
  }

  override protected def read(in: DataInputStream): Unit = {
    val tmpRdmaShuffleManagerIds = new ArrayBuffer[RdmaShuffleManagerId]
    scala.util.control.Exception.ignoring(classOf[EOFException]) {
      while (true) {
        tmpRdmaShuffleManagerIds += RdmaShuffleManagerId(in)
      }
    }
    rdmaShuffleManagerIds = tmpRdmaShuffleManagerIds
  }
}

object RdmaAnnounceRdmaShuffleManagersRpcMsg {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaAnnounceRdmaShuffleManagersRpcMsg()
    obj.read(in)
    obj
  }
}

class RdmaPublishMapTaskOutputRpcMsg(
    var blockManagerId: BlockManagerId,
    var shuffleId: Int,
    var mapId: Int,
    var totalNumPartitions: Int,
    var firstReduceId: Int,
    var lastReduceId: Int,
    var rdmaMapTaskOutput: RdmaMapTaskOutput) extends RdmaRpcMsg {
  import RdmaMapTaskOutput.ENTRY_SIZE
  private var serializableBlockManagerId = if (blockManagerId != null) {
    new SerializableBlockManagerId(blockManagerId)
  } else {
    null
  }

  // 4 bytes each for shuffleId, mapId, totalNumPartitions, startReduceId, lastReduceId
  private def overhead: Int = serializableBlockManagerId.serializedLength + 4 + 4 + 4 + 4 + 4

  private def this() = this(null, 0, -1, 0, 0, 0, null)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.PublishMapTaskOutput

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    var segmentSizes = new ArrayBuffer[Int]
    segmentSizes += overhead

    var remainingPartitions = rdmaMapTaskOutput.getNumPartitions
    while (remainingPartitions > 0) {
      val numLeft = if ((segmentSize - segmentSizes.last) / ENTRY_SIZE > 0) {
        (segmentSize - segmentSizes.last) / ENTRY_SIZE
      } else {
        segmentSizes += overhead
        (segmentSize - segmentSizes.last) / ENTRY_SIZE
      }

      segmentSizes.update(segmentSizes.length - 1,
        segmentSizes.last + Math.min(remainingPartitions, numLeft) * ENTRY_SIZE)
      remainingPartitions -= Math.min(remainingPartitions, numLeft)
    }

    segmentSizes.toArray
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    var curOut: (DataOutputStream, Int) = null
    var curSegmentLength = 0
    var curStartReduceId = firstReduceId
    var curLastReduceId = 0

    def nextOut() {
      curOut = outs.next()
      serializableBlockManagerId.write(curOut._1)
      curOut._1.writeInt(shuffleId)
      curOut._1.writeInt(mapId)
      curOut._1.writeInt(totalNumPartitions)
      curOut._1.writeInt(curStartReduceId)
      curSegmentLength = overhead

      curLastReduceId = curStartReduceId + ((curOut._2 - curSegmentLength) / ENTRY_SIZE) - 1
      require(curLastReduceId <= lastReduceId)
      curOut._1.writeInt(curLastReduceId)
    }

    while (curStartReduceId < rdmaMapTaskOutput.getNumPartitions) {
      nextOut()
      val byteBuffer = rdmaMapTaskOutput.getByteBuffer(curStartReduceId, curLastReduceId)
      val buf = new Array[Byte](byteBuffer.remaining())
      byteBuffer.get(buf)

      curOut._1.write(buf, 0, buf.length)
      curStartReduceId = curLastReduceId + 1
    }
  }

  override protected def read(in: DataInputStream): Unit = {
    serializableBlockManagerId = SerializableBlockManagerId(in)
    blockManagerId = serializableBlockManagerId.toBlockManagerId
    shuffleId = in.readInt()
    mapId = in.readInt()
    totalNumPartitions = in.readInt()
    firstReduceId = in.readInt()
    lastReduceId = in.readInt()

    rdmaMapTaskOutput = new RdmaMapTaskOutput(firstReduceId, lastReduceId)
    for (reduceId <- firstReduceId to lastReduceId) {
      rdmaMapTaskOutput.put(reduceId, in.readLong(), in.readInt(), in.readInt())
    }
  }
}

object RdmaPublishMapTaskOutputRpcMsg {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaPublishMapTaskOutputRpcMsg()
    obj.read(in)
    obj
  }
}

// TODO: cut down size for repeating reduceids - will usually be the same
class RdmaFetchMapStatusRpcMsg(
    var requestingRdmaShuffleManagerId: RdmaShuffleManagerId,
    var requestedBlockManagerId: BlockManagerId,
    var shuffleId: Int,
    var callbackId: Int,
    var mapIdReduceIds: Seq[(Int, Int)]) extends RdmaRpcMsg {
  private var serializableBlockManagerId = if (requestedBlockManagerId != null) {
    new SerializableBlockManagerId(requestedBlockManagerId)
  } else {
    null
  }

  // 4 for shuffleId, 4 for callbackId
  private def overhead: Int = requestingRdmaShuffleManagerId.serializedLength +
    serializableBlockManagerId.serializedLength + 4 + 4

  private val ENTRY_SIZE: Int = 8

  private def this() = this(null, null, -1, -1, null)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.FetchMapStatus

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    val segmentSizes = new ArrayBuffer[Int]
    segmentSizes += overhead

    var curMapIdReduceIdsLength = mapIdReduceIds.length
    while (curMapIdReduceIdsLength > 0) {
      val numLeft = if ((segmentSize - segmentSizes.last) / ENTRY_SIZE > 0) {
        (segmentSize - segmentSizes.last) / ENTRY_SIZE
      } else {
        segmentSizes += overhead
        (segmentSize - segmentSizes.last) / ENTRY_SIZE
      }

      segmentSizes.update(segmentSizes.length - 1,
        segmentSizes.last + Math.min(curMapIdReduceIdsLength, numLeft) * ENTRY_SIZE)
      curMapIdReduceIdsLength -= Math.min(curMapIdReduceIdsLength, numLeft)
    }

    segmentSizes.toArray
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    var curOut: (DataOutputStream, Int) = null
    var curSegmentLength = 0

    def nextOut() {
      curOut = outs.next()
      requestingRdmaShuffleManagerId.write(curOut._1)
      serializableBlockManagerId.write(curOut._1)
      curOut._1.writeInt(shuffleId)
      curOut._1.writeInt(callbackId)
      curSegmentLength = overhead
    }

    nextOut()
    for ((mapId, reduceId) <- mapIdReduceIds) {
      if (curSegmentLength + ENTRY_SIZE > curOut._2) {
        nextOut()
      }
      curSegmentLength += ENTRY_SIZE
      curOut._1.writeInt(mapId)
      curOut._1.writeInt(reduceId)
    }
  }

  override protected def read(in: DataInputStream): Unit = {
    requestingRdmaShuffleManagerId = RdmaShuffleManagerId(in)
    serializableBlockManagerId = SerializableBlockManagerId(in)
    requestedBlockManagerId = serializableBlockManagerId.toBlockManagerId
    shuffleId = in.readInt()
    callbackId = in.readInt()

    val tmpMapIdReduceIds = new ArrayBuffer[(Int, Int)]
    scala.util.control.Exception.ignoring(classOf[EOFException]) {
      while (true) tmpMapIdReduceIds += ((in.readInt(), in.readInt()))
    }
    mapIdReduceIds = tmpMapIdReduceIds
  }
}

object RdmaFetchMapStatusRpcMsg {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaFetchMapStatusRpcMsg()
    obj.read(in)
    obj
  }
}

class RdmaFetchMapStatusResponseRpcMsg(
    var callbackId: Int,
    var requestedRdmaShuffleManagerId: RdmaShuffleManagerId,
    var rdmaBlockLocations: Seq[RdmaBlockLocation]) extends RdmaRpcMsg {
  // 4 for callbackId
  private def overhead: Int = 4 + requestedRdmaShuffleManagerId.serializedLength

  private val ENTRY_SIZE: Int = 16

  private def this() = this(-1, null, null)  // For deserialization only

  override protected def msgType: RdmaRpcMsgType = RdmaRpcMsgType.FetchMapStatusResponse

  override protected def getLengthInSegments(segmentSize: Int): Array[Int] = {
    var segmentSizes = new ArrayBuffer[Int]
    segmentSizes += overhead

    var curRdmaBlockLocationsLength = rdmaBlockLocations.length
    while (curRdmaBlockLocationsLength > 0) {
      val numLeft = if ((segmentSize - segmentSizes.last) / ENTRY_SIZE > 0) {
        (segmentSize - segmentSizes.last) / ENTRY_SIZE
      } else {
        segmentSizes += overhead
        (segmentSize - segmentSizes.last) / ENTRY_SIZE
      }

      segmentSizes.update(segmentSizes.length - 1,
        segmentSizes.last + Math.min(curRdmaBlockLocationsLength, numLeft) * ENTRY_SIZE)
      curRdmaBlockLocationsLength -= Math.min(curRdmaBlockLocationsLength, numLeft)
    }

    segmentSizes.toArray
  }

  override protected def writeSegments(outs: Iterator[(DataOutputStream, Int)]): Unit = {
    var curOut: (DataOutputStream, Int) = null
    var curSegmentLength = 0

    def nextOut() {
      curOut = outs.next()
      curOut._1.writeInt(callbackId)
      requestedRdmaShuffleManagerId.write(curOut._1)
      curSegmentLength = overhead
    }

    nextOut()
    for (rdmaBlockLocation <- rdmaBlockLocations) {
      if (curSegmentLength + ENTRY_SIZE > curOut._2) {
        nextOut()
      }
      curSegmentLength += ENTRY_SIZE
      curOut._1.writeLong(rdmaBlockLocation.address)
      curOut._1.writeInt(rdmaBlockLocation.length)
      curOut._1.writeInt(rdmaBlockLocation.mKey)
    }
  }

  override protected def read(in: DataInputStream): Unit = {
    callbackId = in.readInt()
    requestedRdmaShuffleManagerId = RdmaShuffleManagerId(in)

    val tmpRdmaBlockLocations = new ArrayBuffer[RdmaBlockLocation]
    scala.util.control.Exception.ignoring(classOf[EOFException]) {
      while (true) {
        tmpRdmaBlockLocations += RdmaBlockLocation(in.readLong(), in.readInt(), in.readInt())
      }
    }
    rdmaBlockLocations = tmpRdmaBlockLocations
  }
}

object RdmaFetchMapStatusResponseRpcMsg {
  def apply(in: DataInputStream): RdmaRpcMsg = {
    val obj = new RdmaFetchMapStatusResponseRpcMsg()
    obj.read(in)
    obj
  }
}
