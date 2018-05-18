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
import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.rdma.verbs.SVCRegMr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContextExecutor;


public class RdmaBufferManager {
  private class AllocatorStack {
    private final AtomicInteger totalAlloc = new AtomicInteger(0);
    private final AtomicInteger preAllocs = new AtomicInteger(0);
    private final ConcurrentLinkedDeque<RdmaBuffer> stack = new ConcurrentLinkedDeque<>();
    private final int length;
    private long lastAccess;
    private final AtomicLong idleBuffersSize = new AtomicLong(0);

    private AllocatorStack(int length) {
      this.length = length;
    }

    private int getTotalAlloc() {
      return totalAlloc.get();
    }

    private int getTotalPreAllocs() {
      return preAllocs.get();
    }

    private RdmaBuffer get() throws IOException {
      lastAccess = System.nanoTime();
      RdmaBuffer rdmaBuffer = stack.pollFirst();
      if (rdmaBuffer == null) {
        totalAlloc.getAndIncrement();
        return new RdmaBuffer(getPd(), length);
      } else {
        idleBuffersSize.addAndGet(-length);
        return rdmaBuffer;
      }
    }

    private void put(RdmaBuffer rdmaBuffer) {
      rdmaBuffer.clean();
      lastAccess = System.nanoTime();
      stack.addLast(rdmaBuffer);
      idleBuffersSize.addAndGet(length);
    }

    private void preallocate(int numBuffers) throws IOException {
      logger.debug("Pre allocating {} buffer of size {} KB", numBuffers, length / 1024);
      RdmaBuffer[] preAllocatedBuffers = RdmaBuffer.preAllocate(getPd(), length, numBuffers);
      for (int i = 0; i < numBuffers; i++) {
        put(preAllocatedBuffers[i]);
        preAllocs.getAndIncrement();
      }
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
  private final ConcurrentHashMap<Integer, AllocatorStack> allocStackMap =
    new ConcurrentHashMap<>();
  private IbvPd pd;
  private IbvMr odpMr = null;
  private long maxCacheSize;
  private static final ExecutionContextExecutor globalScalaExecutor =
    ExecutionContext.Implicits$.MODULE$.global();

  RdmaBufferManager(IbvPd pd, boolean isExecutor, RdmaShuffleConf conf) throws IOException {
    this.pd = pd;
    this.minimumAllocationSize = Math.min(conf.recvWrSize(), MIN_BLOCK_SIZE);
    this.maxCacheSize = conf.maxBufferAllocationSize();
    if (conf.useOdp(pd.getContext())) {
      int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE |
        IbvMr.IBV_ACCESS_REMOTE_READ | IbvMr.IBV_ACCESS_ON_DEMAND;

      SVCRegMr sMr = pd.regMr(0, -1, access).execute();
      this.odpMr = sMr.getMr();
      sMr.free();
    }

    int aggBlockPrealloc = (int)(conf.maxAggPrealloc() / conf.maxAggBlock());
    if (aggBlockPrealloc > 0 && isExecutor) {
      AllocatorStack allocatorStack = getOrCreateAllocatorStack((int)conf.maxAggBlock());
      allocatorStack.preallocate(aggBlockPrealloc);
    }
  }

  public void preAllocate(int length, int numBuffers) throws IOException {
    getOrCreateAllocatorStack(length).preallocate(numBuffers);
  }

  private AllocatorStack getOrCreateAllocatorStack(int length) {
    AllocatorStack allocatorStack = allocStackMap.get(length);
    if (allocatorStack == null) {
      allocStackMap.putIfAbsent(length, new AllocatorStack(length));
      allocatorStack = allocStackMap.get(length);
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
    AllocatorStack allocatorStack = allocStackMap.get(buf.getLength());
    if (allocatorStack == null) {
      buf.free();
    } else {
      allocatorStack.put(buf);
      FutureTask<Void> cleaner = new FutureTask<>(() -> {
        // Check the size of current idling buffers
        long idleBuffersSize = allocStackMap
          .reduceValuesToLong(100L, allocStack -> allocStack.idleBuffersSize.get(), 0L, Long::sum);
        // If it reached out 90% of idle buffer capacity, clean old stacks
        if (idleBuffersSize > maxCacheSize * 0.90) {
          cleanLRUStacks(idleBuffersSize);
        }
        return null;
      });
      globalScalaExecutor.execute(cleaner);
    }
  }

  private void cleanLRUStacks(long idleBuffersSize) {
    logger.debug("Current idle buffer size {}KB exceed 90% of maxCacheSize {}KB." +
        " Cleaning LRU idle stacks", idleBuffersSize / 1024, maxCacheSize / 1024);
    AllocatorStack lruStack = allocStackMap.values().stream()
      .sorted(Comparator.comparingLong(s -> s.lastAccess)).iterator().next();
    long totalCleaned = 0;
    // Will clean up to 65% of capacity
    long needToClean = idleBuffersSize - (long) (maxCacheSize * 0.65);
    while (!lruStack.stack.isEmpty() && totalCleaned < needToClean) {
      RdmaBuffer rdmaBuffer = lruStack.stack.pollFirst();
      if (rdmaBuffer != null) {
        rdmaBuffer.free();
        totalCleaned += lruStack.length;
        lruStack.idleBuffersSize.addAndGet(-lruStack.length);
      }
    }
    logger.debug("Cleaned {} KB of idle stacks of size {} KB",
      totalCleaned / 1024, lruStack.length / 1024);
  }

  IbvPd getPd() { return this.pd; }

  IbvMr getOdpMr() { return this.odpMr; }

  void stop() throws IOException {
    logger.info("Rdma buffers allocation statistics:");
    for (Integer size : allocStackMap.keySet()) {
      AllocatorStack allocatorStack = allocStackMap.remove(size);
      if (allocatorStack != null) {
        logger.info( "Pre allocated {}, allocated {} buffers of size {} KB",
            allocatorStack.getTotalPreAllocs(), allocatorStack.getTotalAlloc(), (size / 1024));
        allocatorStack.close();
      }
    }

    if (odpMr != null) {
      odpMr.deregMr().execute().free();
    }
  }
}
