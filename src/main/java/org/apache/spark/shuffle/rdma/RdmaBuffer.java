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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.rdma.verbs.SVCRegMr;
import com.ibm.disni.rdma.verbs.IbvMr;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class RdmaBuffer {
  private static final Logger logger = LoggerFactory.getLogger(RdmaBuffer.class);
  private IbvMr ibvMr = null;
  private final long address;
  private final int length;

  @SuppressWarnings("restriction")
  private static final Unsafe unsafe;

  public static final int BYTE_ARRAY_OFFSET;

  static {
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe)unsafeField.get(null);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      logger.error("Failed to retrieve the Unsafe");
      throw new RuntimeException(e);
    }

    BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
  }

  public RdmaBuffer(IbvPd ibvPd, int length) throws IOException {
    address = unsafe.allocateMemory((long)length);
    this.length = length;
    register(ibvPd);
  }

  public RdmaBuffer(int length) throws IOException {
    address = unsafe.allocateMemory((long)length);
    this.length = length;
  }

  public long getAddress() {
    return address;
  }
  public int getLength() {
    return length;
  }
  public int getLkey() {
    return ibvMr.getLkey();
  }

  public void free() {
    unregister();
    unsafe.freeMemory(address);
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

  public void write(DirectBuffer buf, long srcOffset, long dstOffset, long length) {
    unsafe.copyMemory(buf.address() + srcOffset, getAddress() + dstOffset, length);
  }

  public void write(byte[] bytes, long srcOffset, long dstOffset, long length) {
    unsafe.copyMemory(bytes, BYTE_ARRAY_OFFSET + srcOffset, null, getAddress() + dstOffset, length);
  }

  public ByteBuffer getByteBuffer() throws IOException {
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
