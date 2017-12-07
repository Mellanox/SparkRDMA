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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ibm.disni.rdma.verbs.IbvPd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaBufferManager {
  private class AllocatorStack {
    private final AtomicInteger totalAlloc = new AtomicInteger(0);
    private final ConcurrentLinkedDeque<RdmaBuffer> stack = new ConcurrentLinkedDeque<>();
    private final int length;

    private AllocatorStack(int length) {
      this.length = length;
    }

    private int getTotalAlloc() {
      return totalAlloc.get();
    }

    private RdmaBuffer get() throws IOException {
      RdmaBuffer rdmaBuffer = stack.pollFirst();
      if (rdmaBuffer == null) {
        totalAlloc.getAndIncrement();
        return new RdmaBuffer(getPd(), length);
      } else {
        return rdmaBuffer;
      }
    }

    private void put(RdmaBuffer rdmaBuffer) {
      stack.addLast(rdmaBuffer);
    }

    private void alloc() throws IOException {
      stack.addLast(new RdmaBuffer(getPd(), length));
      totalAlloc.getAndIncrement();
    }

    private void close() {
      while (!stack.isEmpty()) {
        RdmaBuffer rdmaBuffer = stack.poll();
        if (rdmaBuffer != null) {
          rdmaBuffer.free();
        }
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RdmaBufferManager.class);
  private static final int MIN_BLOCK_SIZE = 16 * 1024;

  private final int minimumAllocationSize;
  private final ConcurrentHashMap<Integer, AllocatorStack> stack_map = new ConcurrentHashMap<>();
  private IbvPd pd = null;

  RdmaBufferManager(IbvPd pd, boolean isExecutor, RdmaShuffleConf conf) throws IOException {
    this.pd = pd;
    this.minimumAllocationSize = Math.min(conf.recvWrSize(), MIN_BLOCK_SIZE);

    long aggBlockPrealloc = conf.maxAggPrealloc() / conf.maxAggBlock();
    if (aggBlockPrealloc > 0 && isExecutor) {
      AllocatorStack allocatorStack = getOrCreateAllocatorStack((int)conf.maxAggBlock());
      for (int i = 0; i < aggBlockPrealloc; i++) {
        allocatorStack.alloc();
      }
    }
  }

  private AllocatorStack getOrCreateAllocatorStack(int length) {
    AllocatorStack allocatorStack = stack_map.get(length);
    if (allocatorStack == null) {
      stack_map.putIfAbsent(length, new AllocatorStack(length));
      allocatorStack = stack_map.get(length);
    }

    return allocatorStack;
  }

  RdmaBuffer get(int length) throws IOException {
    // Round up length to the nearest power of two, or the minimum block size
    if (length < minimumAllocationSize) {
      length = minimumAllocationSize;
    } else {
      length--;
      length |= length >> 1;
      length |= length >> 2;
      length |= length >> 4;
      length |= length >> 8;
      length |= length >> 16;
      length++;
    }

    return getOrCreateAllocatorStack(length).get();
  }

  void put(RdmaBuffer buf) {
    AllocatorStack allocatorStack = stack_map.get(buf.getLength());
    if (allocatorStack == null) {
      buf.free();
    } else {
      allocatorStack.put(buf);
    }
  }

  IbvPd getPd() { return this.pd; }

  void stop() {
    logger.info("Rdma buffers allocation statistics:");
    for (Integer size : stack_map.keySet()) {
      AllocatorStack allocatorStack = stack_map.remove(size);
      if (allocatorStack != null) {
        logger.info("Allocated " + allocatorStack.getTotalAlloc() + " buffers of size " +
          (size /1024) + " KB");
        allocatorStack.close();
      }
    }
  }
}
