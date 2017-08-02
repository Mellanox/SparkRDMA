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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.shuffle.rdma.RdmaBuffer

private[spark] class RdmaChunkedByteBuffer(var chunks: ArrayBuffer[(RdmaBuffer, ByteBuffer)]) {
  require(chunks != null, "chunks must not be null")
  require(chunks.forall(_._2.position() == 0), "chunks' positions must be 0")

  private[this] var disposed: Boolean = false

  val length: Long = chunks.map(_._2.limit().asInstanceOf[Long]).sum

  def getChunks(): Array[ByteBuffer] = {
    chunks.map(_._2).toArray
  }

  def getRdmaBufferChunks(): ArrayBuffer[(RdmaBuffer, ByteBuffer)] = chunks

  def dispose(): Unit = {
    if (!disposed) {
      chunks.foreach(_._1.free())
      disposed = true
    }
  }
}