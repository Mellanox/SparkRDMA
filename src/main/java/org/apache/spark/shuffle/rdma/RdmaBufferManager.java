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

import java.util.concurrent.atomic.AtomicBoolean;

public class RdmaBufferManager {

  private class AllocatorStack {
    private final AtomicInteger totalAlloc;
    private final AtomicInteger currentSize;
    private final int length;
    private final ConcurrentLinkedDeque<RdmaBuffer> stack;

    private AllocatorStack(int length) {
      stack = new ConcurrentLinkedDeque<>();
      totalAlloc = new AtomicInteger(0);
      currentSize = new AtomicInteger(0);
      this.length = length;
    }

    private int getTotalAlloc() {
      return totalAlloc.get();
    }

    private int getCurrentSize() {
      return currentSize.get();
    }

    private RdmaBuffer getStackBuf() throws IOException {
      if (currentSize.getAndDecrement() > 0 && !stack.isEmpty())
        return stack.pop();
      else {
        currentSize.getAndIncrement();
        return new RdmaBuffer(getPd(), getLength());
      }
    }

    private void putStackBuf(RdmaBuffer rB) {
      stack.addLast(rB);
      currentSize.getAndIncrement();
    }

    private void addStackBuf(RdmaBuffer rB) {
      stack.push(rB);
      totalAlloc.getAndIncrement();
      currentSize.getAndIncrement();
    }

    private int getLength() {
      return length;
    }

    private void emptyStackBufs() {
      while (!stack.isEmpty()) {
        RdmaBuffer rB = stack.pop();
        rB.free();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RdmaBufferManager.class);
  private static final int MIN_BLOCK_SIZE = 16 * 1024;

  private final ConcurrentHashMap<Integer, AllocatorStack> stack_map = new ConcurrentHashMap<>();
  private final AllocatorStack recvStack;
  private IbvPd pd = null;
  private Thread allocThread;
  private final AtomicBoolean isThreadRunning  = new AtomicBoolean(false);
  private final AtomicBoolean allocMore = new AtomicBoolean(false);
  private final ConcurrentLinkedDeque<Integer> lenBufQ = new ConcurrentLinkedDeque<>();
  private final boolean allocMoreThread = false;

  RdmaBufferManager(IbvPd pd, final boolean isExecutor, RdmaShuffleConf conf) throws IOException {
    long maxAggBlock = conf.maxAggBlock();
    this.pd = pd;

    recvStack = new AllocatorStack(conf.recvWrSize());
    for (int i = 0; i < conf.recvQueueDepth(); i++) {
      alloc(recvStack);
    }

    long aggBlockPrealloc = conf.maxAggPrealloc() / maxAggBlock;
    AllocatorStack aStack = alloc((int) maxAggBlock);
    for (int i = 0; i < aggBlockPrealloc; i++) {
      alloc(aStack);
    }

    if (allocMoreThread) {
      allocThread = new Thread(() -> {
        AllocatorStack aStack1 = null;

        if (isExecutor) {
          for (int sz = MIN_BLOCK_SIZE; sz < MIN_BLOCK_SIZE * 64; sz *= 2) {
            try {
              aStack1 = alloc(sz);
            } catch (IOException e) {
              e.printStackTrace();
            }
            if (aStack1 != null) {
              for (int i = 0; i < 100; i++) {
                try {
                  alloc(aStack1);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            }
            logger.info("Pre-Allocate 100 of " + sz / 1024 + " KB-buffers");
          }
          allocMore.set(false);
        }

        while (isThreadRunning.get()) {
          try {
            if (allocMore.get()) {
              while (!lenBufQ.isEmpty()) {
                long t2, t1;
                int len = lenBufQ.pop();

                aStack1 = alloc(len);

                if (aStack1.getCurrentSize() > 99)
                  continue;

                t1 = System.nanoTime();
                for (int i = 0; i < 50; i++)
                  alloc(aStack1);
                t2 = System.nanoTime();
                logger.info("Allocate 50 more " + len / 1024 + " KB-buffers in " +
                  (t2 - t1) / 1e6 + " ms totalAlloc " + aStack1.getTotalAlloc());
              }
              allocMore.set(false);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }

          try {
            synchronized (allocMore) {
              allocMore.wait();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }, "RdmaBufferManager allocation thread");

      allocMore.set(true);
      isThreadRunning.set(true);
      allocThread.start();
    }
  }

  private void alloc(AllocatorStack aStack) throws IOException {
    aStack.addStackBuf(new RdmaBuffer(getPd(), aStack.getLength()));
  }

  private AllocatorStack alloc(int length) throws IOException {
    AllocatorStack aStack = stack_map.get(length);
    if (aStack == null) {
      stack_map.putIfAbsent(length, new AllocatorStack(length));
      aStack = stack_map.get(length);
    }

    alloc(aStack);

    return aStack;
  }

  void putRecv(RdmaBuffer buf) {
    recvStack.putStackBuf(buf);
  }

  RdmaBuffer getRecv() throws IOException {
    return recvStack.getStackBuf();
  }

  RdmaBuffer get(int length, boolean isAggBlock) throws IOException {
    AllocatorStack newAStack;

    if (length < MIN_BLOCK_SIZE) {
      length = MIN_BLOCK_SIZE;
    } else if (!isAggBlock) {
      length--;
      length |= length >> 1;
      length |= length >> 2;
      length |= length >> 4;
      length |= length >> 8;
      length |= length >> 16;
      length++;
    }


    RdmaBuffer buf;
    newAStack = stack_map.get(length);
    /*
    if (newAStack == null || newAStack.getCurrentSize() < 50) {
      // always alloc 1 first before kick-off the thread to alloc more
      newAStack = alloc(length);
      buf = newAStack.getStackBuf();

      if (allocMoreThread && !isAggBlock) {
        lenBufQ.addLast(length);
        synchronized (allocMore) {
          if (!allocMore.get()) {
            allocMore.set(true);
            allocMore.notify();
          }
        }
      }
    } else {
      buf  = newAStack.getStackBuf();
    }
    */

    if (newAStack == null) {
      newAStack = alloc(length);
    }
    buf = newAStack.getStackBuf();

    return buf;
  }

  void put(RdmaBuffer buf) {
    AllocatorStack aStack = stack_map.get(buf.getLength());
    if (aStack == null) {
      buf.free();
    } else {
      aStack.putStackBuf(buf);
    }
  }

  private void printMaxNumBufsAlloc() {
    for (int sz = MIN_BLOCK_SIZE; sz < MIN_BLOCK_SIZE * 64; sz *= 2) {
      AllocatorStack aStack = stack_map.get(sz);
      logger.info(" " + sz / 1024 + "KB_bufs= " + ((aStack == null) ? 0 : aStack.getTotalAlloc()));
    }
  }

  public IbvPd getPd() {
    return this.pd;
  }

  public void stop() throws InterruptedException {
    if (allocMoreThread) {
      isThreadRunning.set(false);
      synchronized (allocMore) {
        allocMore.notify();
      }
      allocThread.join();
    }

    printMaxNumBufsAlloc();

    for (Integer size : stack_map.keySet()) {
      AllocatorStack aStack = stack_map.remove(size);
      if (aStack != null) {
        aStack.emptyStackBufs();
      }
    }
    recvStack.emptyStackBufs();
  }
}