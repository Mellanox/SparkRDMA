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

import java.io.OutputStream
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.shuffle.rdma.RdmaBuffer

private[spark] class RdmaChunkedByteBufferOutputStream(chunkSize: Int) extends OutputStream {
  def this(chunkSize: Int, arrBuf: ArrayBuffer[(RdmaBuffer, ByteBuffer)]) {
    this(chunkSize)
    chunks = arrBuf
    chunks.foreach(_._2.clear())
  }

  private[this] var toChunkedByteBufferWasCalled = false

  private var chunks = new ArrayBuffer[(RdmaBuffer, ByteBuffer)]

  private def allocator(length: Int): (RdmaBuffer, ByteBuffer) = {
    val rdmaBuf = new RdmaBuffer(length)
    (rdmaBuf, rdmaBuf.getByteBuffer)
  }

  private[this] var lastChunkIndex = -1

  private[this] var position = chunkSize
  private[this] var _size = 0

  def size: Long = _size

  override def write(b: Int): Unit = {
    allocateNewChunkIfNeeded()
    chunks(lastChunkIndex)._2.put(b.toByte)
    position += 1
    _size += 1
  }

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    var written = 0
    while (written < len) {
      allocateNewChunkIfNeeded()
      val thisBatch = math.min(chunkSize - position, len - written)
      chunks(lastChunkIndex)._2.put(bytes, written + off, thisBatch)
      written += thisBatch
      position += thisBatch
    }
    _size += len
  }

  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    require(!toChunkedByteBufferWasCalled, "cannot write after toChunkedByteBuffer() is called")
    if (position == chunkSize) {
      lastChunkIndex += 1
      if (chunks.size == lastChunkIndex) {
        chunks += allocator(chunkSize)
      }
      position = 0
    }
  }

  def toRdmaChunkedByteBuffer: RdmaChunkedByteBuffer = {
    require(!toChunkedByteBufferWasCalled, "toChunkedByteBuffer() can only be called once")
    toChunkedByteBufferWasCalled = true
    if (lastChunkIndex == -1) {
      chunks.foreach(_._1.free())
      new RdmaChunkedByteBuffer(ArrayBuffer.empty[(RdmaBuffer, ByteBuffer)])
    } else {
      val ret = new ArrayBuffer[(RdmaBuffer, ByteBuffer)]
      for (i <- 0 to lastChunkIndex) {
        ret += chunks(i)
        ret(i)._2.flip()
      }

      for (i <- lastChunkIndex + 1 until chunks.size) {
        chunks(i)._1.free()
      }

      new RdmaChunkedByteBuffer(ret)
    }
  }
}