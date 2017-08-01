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

import com.ibm.disni.rdma.verbs.IbvPd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RdmaRegisteredBuffer {
  private RdmaBufferManager rdmaBufferManager = null;
  private RdmaBuffer rdmaBuffer;
  private AtomicInteger refCount = new AtomicInteger(0);
  private int blockOffset = 0;

  public RdmaRegisteredBuffer(RdmaBufferManager rdmaBufferManager, int length, boolean isAggBlock)
      throws IOException {
    this.rdmaBufferManager = rdmaBufferManager;
    this.rdmaBuffer = rdmaBufferManager.get(length, isAggBlock);
    assert this.rdmaBuffer != null;
  }

  public RdmaRegisteredBuffer(IbvPd ibvPd, int length) throws IOException {
    this.rdmaBuffer = new RdmaBuffer(ibvPd, length);
  }

  int getLkey() {
    return rdmaBuffer.getLkey();
  }

  public int retain() {
    return refCount.incrementAndGet();
  }

  public int release() {
    int count = refCount.decrementAndGet();

    if (count == 0) {
      if (rdmaBufferManager != null) {
        rdmaBufferManager.put(rdmaBuffer);
      } else {
        rdmaBuffer.free();
      }
      rdmaBuffer = null;
    }

    return count;
  }

  private long getRegisteredAddress() {
    return rdmaBuffer.getAddress();
  }

  private int getRegisteredLength() {
    return rdmaBuffer.getLength();
  }

  ByteBuffer getByteBuffer(int length) {
    if (blockOffset + length > getRegisteredLength()) {
      throw new IllegalArgumentException("Exceeded Registered Length!");
    }

    ByteBuffer byteBuffer = null;
    try {
      Class<?> classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
      constructor.setAccessible(true);
      byteBuffer = (ByteBuffer)constructor.newInstance(getRegisteredAddress() + (long)blockOffset, length);
      blockOffset += length;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return byteBuffer;
  }
}