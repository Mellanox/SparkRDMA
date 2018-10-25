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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.UnsafeMemoryAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.rdma.verbs.SVCRegMr;
import com.ibm.disni.rdma.verbs.IbvMr;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class RdmaBuffer {
  private static final Logger logger = LoggerFactory.getLogger(RdmaBuffer.class);

  private IbvMr ibvMr;
  private final long address;
  private final int length;
  private final MemoryBlock block;
  private AtomicInteger refCount;

  static final UnsafeMemoryAllocator unsafeAlloc = new UnsafeMemoryAllocator();
  public static final Constructor<?> directBufferConstructor;

  static {
    try {
      Class<?> classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
      directBufferConstructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
      directBufferConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("java.nio.DirectByteBuffer class not found");
    }
  }

  RdmaBuffer(IbvPd ibvPd, int length) throws IOException {
    block = unsafeAlloc.allocate((long)length);
    address = block.getBaseOffset();
    this.length = length;
    refCount = new AtomicInteger(1);
    clean();
    ibvMr = register(ibvPd, address, length);
  }

  private RdmaBuffer(IbvMr ibvMr, AtomicInteger refCount, long address, int length,
      MemoryBlock block) {
    this.ibvMr = ibvMr;
    this.refCount = refCount;
    this.address = address;
    this.length = length;
    this.block = block;
  }

  public void clean() {
    Platform.setMemory(address, (byte)0, length);
  }

  /**
   * Pre allocates @numBlocks buffers of size @length under single MR.
   * @param ibvPd
   * @param length
   * @param numBlocks
   * @return
   * @throws IOException
   */
  public static RdmaBuffer[] preAllocate(IbvPd ibvPd, int length, int numBlocks)
      throws IOException {
    MemoryBlock block = unsafeAlloc.allocate(length * numBlocks);
    long baseAddress = block.getBaseOffset();
    IbvMr ibvMr = register(ibvPd, baseAddress, length * numBlocks);
    RdmaBuffer[] result = new RdmaBuffer[numBlocks];
    AtomicInteger refCount = new AtomicInteger(numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      result[i] = new RdmaBuffer(ibvMr, refCount, baseAddress + i * length, length, block);
    }
    return result;
  }

  long getAddress() {
    return address;
  }

  int getLength() {
    return length;
  }

  int getLkey() {
    return ibvMr.getLkey();
  }

  void free() {
    if (refCount.decrementAndGet() == 0) {
      unregister();
      unsafeAlloc.free(block);
    }
  }

  private static IbvMr register(IbvPd ibvPd, long address, int length) throws IOException {
    int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE |
      IbvMr.IBV_ACCESS_REMOTE_READ;

    SVCRegMr sMr = ibvPd.regMr(address, length, access).execute();
    IbvMr ibvMr = sMr.getMr();
    sMr.free();
    return ibvMr;
  }

  private void unregister() {
    if (ibvMr != null) {
      try {
        ibvMr.deregMr().execute().free();
      } catch (IOException e) {
        logger.warn("Deregister MR failed");
      }
      ibvMr = null;
    }
  }

  ByteBuffer getByteBuffer() throws IOException {
    try {
      return (ByteBuffer)directBufferConstructor.newInstance(getAddress(), getLength());
    } catch (Exception e) {
      throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
    }
  }
}
