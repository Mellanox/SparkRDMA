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

package org.apache.spark.shuffle.rdma.writer.chunkedpartitionagg

import java.io.{File, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rdma._

class RdmaShufflePartitionWriter(rdmaShuffleManager: RdmaShuffleManager) extends Logging {
  private lazy val blockManager = SparkEnv.get.blockManager
  private final val blockSize = rdmaShuffleManager.rdmaShuffleConf.shuffleWriteBlockSize
  // TODO: we assume blockSize will always be big enough

  case class RdmaWriterBlockAndOffset(writerBlock: RdmaWriterBlock, var offset: Long)
  private val rdmaWriterBlocks = new ArrayBuffer[RdmaWriterBlockAndOffset]

  private def generateFileName: File = {
    blockManager.diskBlockManager.getFile("RdmaShuffleDataFile_" + UUID.randomUUID())
  }

  @inline
  private def addNewBlock() {
    if (rdmaShuffleManager.shuffleBlockResolver.reserveInMemoryBytes(blockSize)) {
      // TODO: these should be taken from the pool, for better reuse with shuffle read
      rdmaWriterBlocks += RdmaWriterBlockAndOffset(
        new RdmaMemoryWriterBlock(rdmaShuffleManager.getIbvPd, blockSize), 0)
    } else {
      rdmaWriterBlocks += RdmaWriterBlockAndOffset(
        new RdmaFileWriterBlock(rdmaShuffleManager.getIbvPd, blockSize, generateFileName), 0)
    }
  }

  @inline
  private def getWriterBlockAndOffset(length: Long): (RdmaWriterBlock, Long) = {
    rdmaWriterBlocks.lastOption match {
      case Some(block) =>
        if (block.offset + length <= blockSize) {
          block.offset += length
          (block.writerBlock, block.offset - length)
        } else {
          addNewBlock()
          rdmaWriterBlocks.last.offset += length
          (rdmaWriterBlocks.last.writerBlock, rdmaWriterBlocks.last.offset - length)
        }

      case None =>
        addNewBlock()
        rdmaWriterBlocks.last.offset += length
        (rdmaWriterBlocks.last.writerBlock, rdmaWriterBlocks.last.offset - length)
    }
  }

  def write(bufs: Array[ByteBuffer], totalLength: Long) {
    require(totalLength <= blockSize, "shuffleWriteBlockSize is too small, received a request to" +
      "write" + totalLength + " bytes")

    val (rdmaWriterBlock, startWriteOffset) = this.synchronized {
      getWriterBlockAndOffset(totalLength)
    }

    rdmaWriterBlock.write(bufs, startWriteOffset)
  }

  def write(buf: ByteBuffer, length: Long) {
    require(length <= blockSize, "shuffleWriteBlockSize is too small, received a request to" +
      "write" + length + " bytes")

    val (rdmaWriterBlock, startWriteOffset) = this.synchronized { getWriterBlockAndOffset(length) }

    rdmaWriterBlock.write(buf, startWriteOffset)
  }

  def writeUnsafe(byte: Integer) {
    val (rdmaWriterBlock, startWriteOffset) = getWriterBlockAndOffset(1)

    rdmaWriterBlock.write(byte, startWriteOffset)
  }

  def writeUnsafe(bytes: Array[Byte]) {
    require(bytes.length <= blockSize, "shuffleWriteBlockSize is too small, received a request to" +
      "write" + bytes.length + " bytes")

    val (rdmaWriterBlock, startWriteOffset) = getWriterBlockAndOffset(bytes.length)

    rdmaWriterBlock.write(bytes, startWriteOffset)
  }

  def getLocations: Seq[RdmaBlockLocation] = rdmaWriterBlocks.map(_.writerBlock.getLocation)

  def dispose(): Unit = {
    for (r <- rdmaWriterBlocks) {
      if (r.writerBlock.isInstanceOf[RdmaMemoryWriterBlock]) {
        rdmaShuffleManager.shuffleBlockResolver.releaseInMemoryBytes(blockSize)
      }
      r.writerBlock.dispose()
    }
  }

  def getInputStreams: Seq[InputStream] = rdmaWriterBlocks.map(_.writerBlock.getInputStream)

  def getOutputStream: OutputStream = {
    new OutputStream {
      override def write(b: Int): Unit = { writeUnsafe(b) }
      override def write(b: Array[Byte]): Unit = { writeUnsafe(b) }
    }
  }

  def getLength: Long = { rdmaWriterBlocks.map(_.offset).sum }
}