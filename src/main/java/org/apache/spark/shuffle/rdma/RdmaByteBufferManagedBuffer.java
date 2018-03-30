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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.spark.network.buffer.ManagedBuffer;

public final class RdmaByteBufferManagedBuffer extends RdmaManagedBuffer {
  private final RdmaRegisteredBuffer rdmaRegisteredBuffer;
  private ByteBuffer byteBuffer;

  public RdmaByteBufferManagedBuffer(RdmaRegisteredBuffer rdmaRegisteredBuffer, int length)
      throws IOException {
    this.rdmaRegisteredBuffer = rdmaRegisteredBuffer;
    this.byteBuffer = rdmaRegisteredBuffer.getByteBuffer(length);
    this.byteBuffer.limit(length);
    retain();
  }

  @Override
  public long size() {
    return byteBuffer.remaining();
  }

  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    return byteBuffer;
  }

  @Override
  public InputStream createInputStream() throws IOException {
    return new ByteBufferBackedInputStream(byteBuffer);
  }

  public OutputStream createOutputStream() throws IOException {
    return new ByteBufferBackedOutputStream(byteBuffer);
  }

  @Override
  public ManagedBuffer retain() {
    rdmaRegisteredBuffer.retain();
    return this;
  }

  @Override
  public ManagedBuffer release() {
    rdmaRegisteredBuffer.release();
    return this;
  }

  @Override
  public long getAddress() {
    return ((sun.nio.ch.DirectBuffer)byteBuffer).address();
  }

  @Override
  public int getLkey() {
    return rdmaRegisteredBuffer.getLkey();
  }

  @Override
  public long getLength() {
    return byteBuffer.capacity();
  }

  @Override
  public Object convertToNetty() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("rdmaRegisteredBuffer", rdmaRegisteredBuffer)
      .toString();
  }
}
