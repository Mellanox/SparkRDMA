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

import java.io.{File, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.ibm.disni.rdma.verbs.IbvPd
import sun.nio.ch.DirectBuffer

import org.apache.spark.shuffle.rdma.{ByteBufferBackedInputStream, RdmaBlockLocation, RdmaBuffer, RdmaSyncFile}

trait RdmaWriterBlock {
  def write(bufs: Array[ByteBuffer], offset: Long)
  def write(buf: ByteBuffer, offset: Long)
  def write(byte: Integer, offset: Long)
  def write(bytes: Array[Byte], offset: Long)
  def getLocation: RdmaBlockLocation
  def getInputStream: InputStream
  def dispose()
}

class RdmaMemoryWriterBlock(ibvPd: IbvPd, blockSize: Long) extends RdmaWriterBlock {
  private val rdmaBuffer = new RdmaBuffer(ibvPd, blockSize.toInt)
  private var actualReadableLength = new AtomicInteger(0)

  override def write(bufs: Array[ByteBuffer], offset: Long): Unit = {
    var curWriteOffset = offset
    for (buf <- bufs) {
      if (buf.isInstanceOf[DirectBuffer]) {
        rdmaBuffer.write(buf.asInstanceOf[DirectBuffer], 0, curWriteOffset, buf.limit())
      } else if (buf.hasArray) {
        rdmaBuffer.write(buf.array(), 0, curWriteOffset, buf.limit())
      } else {
        throw new UnsupportedOperationException("Unsupported ByteBuffer")
      }
      curWriteOffset += buf.limit()
    }

    actualReadableLength.addAndGet((curWriteOffset - offset).toInt)
  }

  override def write(buf: ByteBuffer, offset: Long): Unit = {
    if (buf.isInstanceOf[DirectBuffer]) {
      rdmaBuffer.write(buf.asInstanceOf[DirectBuffer], 0, offset, buf.limit())
    } else if (buf.hasArray) {
      rdmaBuffer.write(buf.array(), 0, offset, buf.limit())
    } else {
      throw new UnsupportedOperationException("Unsupported ByteBuffer")
    }

    actualReadableLength.addAndGet(buf.limit())
  }

  override def write(byte: Integer, offset: Long): Unit = {
    rdmaBuffer.write(Array(byte.byteValue()), 0, offset, 1)

    actualReadableLength.addAndGet(1)
  }

  override def write(bytes: Array[Byte], offset: Long): Unit = {
    rdmaBuffer.write(bytes, 0, offset, bytes.length)

    actualReadableLength.addAndGet(bytes.length)
  }

  override def getLocation: RdmaBlockLocation = RdmaBlockLocation(rdmaBuffer.getAddress,
    actualReadableLength.get(), rdmaBuffer.getLkey)

  override def getInputStream: InputStream = {
    val buf = rdmaBuffer.getByteBuffer
    buf.limit(actualReadableLength.intValue())
    new ByteBufferBackedInputStream(buf)
  }

  override def dispose(): Unit = rdmaBuffer.free()
}

class RdmaFileWriterBlock(ibvPd: IbvPd, blockSize: Long, file: File) extends RdmaWriterBlock {
  private val rdmaSyncFile = new RdmaSyncFile(file, ibvPd, blockSize.toInt)
  private var actualReadableLength = new AtomicInteger(0)

  override def write(bufs: Array[ByteBuffer], offset: Long): Unit = {
    var curWriteOffset = offset
    for (buf <- bufs) {
      if (buf.isInstanceOf[DirectBuffer]) {
        rdmaSyncFile.write(buf.asInstanceOf[DirectBuffer], 0, curWriteOffset, buf.limit())
      } else if (buf.hasArray) {
        rdmaSyncFile.write(buf.array(), 0, curWriteOffset, buf.limit())
      } else {
        throw new UnsupportedOperationException("Unsupported ByteBuffer")
      }
      curWriteOffset += buf.limit()
    }

    actualReadableLength.addAndGet((curWriteOffset - offset).toInt)
  }

  override def write(buf: ByteBuffer, offset: Long): Unit = {
    if (buf.isInstanceOf[DirectBuffer]) {
      rdmaSyncFile.write(buf.asInstanceOf[DirectBuffer], 0, offset, buf.limit())
    } else if (buf.hasArray) {
      rdmaSyncFile.write(buf.array(), 0, offset, buf.limit())
    } else {
      throw new UnsupportedOperationException("Unsupported ByteBuffer")
    }

    actualReadableLength.addAndGet(buf.limit())
  }

  override def write(byte: Integer, offset: Long): Unit = {
    rdmaSyncFile.write(Array(byte.byteValue()), 0, offset, 1)

    actualReadableLength.addAndGet(1)
  }

  override def write(bytes: Array[Byte], offset: Long): Unit = {
    rdmaSyncFile.write(bytes, 0, offset, bytes.length)

    actualReadableLength.addAndGet(bytes.length)
  }

  override def getLocation: RdmaBlockLocation = rdmaSyncFile.getRdmaBlockLocationForOffset(0,
    actualReadableLength.get())

  override def getInputStream: InputStream = {
    val buf = rdmaSyncFile.getByteBuffer
    buf.limit(actualReadableLength.get())
    new ByteBufferBackedInputStream(buf)
  }

  override def dispose(): Unit = rdmaSyncFile.dispose()
}