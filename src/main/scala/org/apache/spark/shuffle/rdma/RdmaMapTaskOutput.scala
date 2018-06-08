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

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}

import org.apache.spark.internal.Logging

object RdmaMapTaskOutput {
  private[rdma] val ENTRY_SIZE = 8 + 4 + 4
  // Only address and key. For map output we don't need a length
  private[rdma] val MAP_ENTRY_SIZE = 8 + 4
}

class RdmaMapTaskOutput private[rdma](
    val startPartitionId: Int,
    val lastPartitionId: Int) extends Logging {
  import RdmaMapTaskOutput.ENTRY_SIZE
  if (startPartitionId > lastPartitionId) {
    throw new IllegalArgumentException("Reduce partition range must be positive")
  }

  private[rdma] def getNumPartitions: Int = lastPartitionId - startPartitionId + 1
  private[rdma] def size: Int = getNumPartitions * ENTRY_SIZE

  final private val fillCount = new AtomicInteger(getNumPartitions)
  final private val byteBuffer = ByteBuffer.allocate(getNumPartitions * ENTRY_SIZE)
  final private val fillPromise = Promise[Unit]
  final val fillFuture: Future[Unit] = fillPromise.future

  private[rdma] def getRdmaBlockLocation(requestedId: Int) = {
    if (requestedId < startPartitionId || requestedId > lastPartitionId) {
      throw new IndexOutOfBoundsException("PartitionId " + requestedId + " is out of range (" +
        startPartitionId + "-" + lastPartitionId + ")")
    }
    RdmaBlockLocation(
      byteBuffer.getLong((requestedId - startPartitionId) * ENTRY_SIZE),
      byteBuffer.getInt((requestedId - startPartitionId) * ENTRY_SIZE + 8),
      byteBuffer.getInt((requestedId - startPartitionId) * ENTRY_SIZE + 8 + 4))
  }

  private[rdma] def getByteBuffer(firstRequestedId: Int, lastRequestedId: Int) = {
    if (firstRequestedId < startPartitionId ||
        lastRequestedId > lastPartitionId ||
        firstRequestedId > lastRequestedId) {
      throw new IndexOutOfBoundsException("StartPartitionId " + firstRequestedId +
        ", LastPartitionId " + lastRequestedId + " are out of range (" + startPartitionId +
        "-" + lastPartitionId + ")")
    }
    byteBuffer.duplicate.position(
      (firstRequestedId - startPartitionId) * ENTRY_SIZE).limit(
        (lastRequestedId - startPartitionId + 1) * ENTRY_SIZE).asInstanceOf[ByteBuffer]
  }

  private[this] def putInternal(partitionId: Int, address: Long, length: Int, mKey: Int) = {
    byteBuffer.putLong((partitionId - startPartitionId) * ENTRY_SIZE, address)
    byteBuffer.putInt((partitionId - startPartitionId) * ENTRY_SIZE + 8, length)
    byteBuffer.putInt((partitionId - startPartitionId) * ENTRY_SIZE + 8 + 4, mKey)
  }

  private[rdma] def put(requestedId: Int, address: Long, length: Int, mKey: Int) = {
    if (requestedId < startPartitionId || requestedId > lastPartitionId) {
      throw new IndexOutOfBoundsException("PartitionId " + requestedId + " is out of range (" +
        startPartitionId + "-" + lastPartitionId + ")")
    }
    putInternal(requestedId, address, length, mKey)
    if (fillCount.decrementAndGet == 0) {
      fillPromise.trySuccess()
    }
  }

  private[rdma] def putRange(firstRequestedId: Int, lastRequestedId: Int, buf: ByteBuffer) = {
    if (firstRequestedId < startPartitionId ||
        lastRequestedId > lastPartitionId ||
        firstRequestedId > lastRequestedId) {
      throw new IndexOutOfBoundsException("StartPartitionId " + firstRequestedId +
        ", LastPartitionId " + lastRequestedId + " are out of range (" + startPartitionId +
        "-" + lastPartitionId + ")")
    }

    for (partId <- firstRequestedId to lastRequestedId) {
      putInternal(partId, buf.getLong, buf.getInt, buf.getInt)
    }

    if (fillCount.addAndGet(-(lastRequestedId - firstRequestedId + 1)) == 0) {
      fillPromise.trySuccess()
    }
  }
}
