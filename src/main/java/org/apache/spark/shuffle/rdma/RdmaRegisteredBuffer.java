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

package org.apache.spark.shuffle.rdma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RdmaRegisteredBuffer {
  private RdmaBufferManager rdmaBufferManager = null;
  private RdmaBuffer rdmaBuffer;
  private final AtomicInteger refCount = new AtomicInteger(0);
  private int blockOffset = 0;

  public RdmaRegisteredBuffer(RdmaBufferManager rdmaBufferManager, int length)
      throws IOException {
    this.rdmaBufferManager = rdmaBufferManager;
    this.rdmaBuffer = rdmaBufferManager.get(length);
    assert this.rdmaBuffer != null;
  }

  int getLkey() {
    return rdmaBuffer.getLkey();
  }

  void retain() {
    refCount.incrementAndGet();
  }

  void release() {
    int count = refCount.decrementAndGet();

    if (count <= 0) {
      free();
    }
  }

  private synchronized void free() {
    if (rdmaBuffer != null) {
      if (rdmaBufferManager != null) {
        rdmaBufferManager.put(rdmaBuffer);
      } else {
        rdmaBuffer.free();
      }
      rdmaBuffer = null;
    }
  }

  long getRegisteredAddress() {
    return rdmaBuffer.getAddress();
  }

  private int getRegisteredLength() {
    return rdmaBuffer.getLength();
  }

  ByteBuffer getByteBuffer(int length) throws IOException {
    if (blockOffset + length > getRegisteredLength()) {
      throw new IllegalArgumentException("Exceeded Registered Length!");
    }

    ByteBuffer byteBuffer;
    try {
      byteBuffer = (ByteBuffer)RdmaBuffer.directBufferConstructor.newInstance(
        getRegisteredAddress() + (long)blockOffset, length);
      blockOffset += length;
    } catch (Exception e) {
      throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
    }

    return byteBuffer;
  }
}
