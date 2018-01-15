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

class RdmaBuffer {
  private static final Logger logger = LoggerFactory.getLogger(RdmaBuffer.class);

  private IbvMr ibvMr = null;
  private final long address;
  private final int length;
  private final MemoryBlock block;

  public static final UnsafeMemoryAllocator unsafeAlloc = new UnsafeMemoryAllocator();

  RdmaBuffer(IbvPd ibvPd, int length) throws IOException {
    block = unsafeAlloc.allocate((long)length);
    address = block.getBaseOffset();
    this.length = length;
    register(ibvPd);
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
    unregister();
    unsafeAlloc.free(block);
  }

  private void register(IbvPd ibvPd) throws IOException {
    int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE |
      IbvMr.IBV_ACCESS_REMOTE_READ;

    SVCRegMr sMr = ibvPd.regMr(getAddress(), getLength(), access).execute();
    ibvMr = sMr.getMr();
    sMr.free();
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
    Class<?> classDirectByteBuffer;
    try {
      classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
    } catch (ClassNotFoundException e) {
      throw new IOException("java.nio.DirectByteBuffer class not found");
    }
    Constructor<?> constructor;
    try {
      constructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
    } catch (NoSuchMethodException e) {
      throw new IOException("java.nio.DirectByteBuffer constructor not found");
    }
    constructor.setAccessible(true);
    ByteBuffer byteBuffer;
    try {
      byteBuffer = (ByteBuffer)constructor.newInstance(getAddress(), getLength());
    } catch (Exception e) {
      throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
    }

    return byteBuffer;
  }
}
