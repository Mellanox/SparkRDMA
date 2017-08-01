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
		private AtomicInteger totalAlloc;
		private AtomicInteger currentSize;
		private int length;
		ConcurrentLinkedDeque<RdmaBuffer> stack;

		public AllocatorStack(Integer length) {
			stack = new ConcurrentLinkedDeque<>();
			totalAlloc = new AtomicInteger(0);
			currentSize = new AtomicInteger(0);
			this.length = length;
		}
		public int getTotalAlloc() { return totalAlloc.get(); }
		public int getCurrentSize() { return currentSize.get(); }
		public RdmaBuffer getStackBuf() throws IOException {
			if (currentSize.getAndDecrement() > 0 && !stack.isEmpty())
				return stack.pop();
			else {
				currentSize.getAndIncrement();
				return new RdmaBuffer(getPd(), getLength());
			}
		}
		public void putStackBuf(RdmaBuffer rB) {
			stack.addLast(rB);
			currentSize.getAndIncrement();
		}
		public void addStackBuf(RdmaBuffer rB) {
			stack.push(rB);
			totalAlloc.getAndIncrement();
			currentSize.getAndIncrement();
		}
		public int getLength() {
			return length;
		}
		private void emptyStackBufs() {
			while (!stack.isEmpty()) {
				RdmaBuffer rB = stack.pop();
				rB.free();
			}
		}
	}

	private static final int MIN_BLOCK_SIZE = 16 * 1024;
	private ConcurrentHashMap<Integer, AllocatorStack> stack_map;
	private AllocatorStack recvStack;
	private	IbvPd pd = null;
	private static final Logger logger = LoggerFactory.getLogger(RdmaBufferManager.class);
	private Thread allocThread;
	private AtomicBoolean isThreadRunning;
	private AtomicBoolean allocMore;
	private final ConcurrentLinkedDeque<Integer> lenBufQ;
	private long maxAggBlock;
	private final boolean allocMoreThread = false;

	public RdmaBufferManager(IbvPd pd, boolean isEx, RdmaShuffleConf conf) throws IOException {
		final boolean isExecutor = isEx;
		lenBufQ = new ConcurrentLinkedDeque<Integer>();
		allocMore = new AtomicBoolean(false);
		isThreadRunning = new AtomicBoolean(false);
		this.maxAggBlock = conf.maxAggBlock();
		this.pd = pd;
		logger.info("Max Agg Block set to: " + maxAggBlock);

		stack_map = new ConcurrentHashMap<Integer, AllocatorStack>();
		recvStack = new AllocatorStack(conf.recvWrSize());
		for (int i = 0; i < conf.recvQueueDepth(); i++) {
			alloc(recvStack);
		}

		long aggBlockPrealloc = conf.maxAggPrealloc()/maxAggBlock;
		AllocatorStack aStack = alloc((int)maxAggBlock);
		for (int i = 0; i < aggBlockPrealloc; i++) {
			alloc(aStack);
		}
		logger.info("Allocated {} Max Agg Block buffers.", aggBlockPrealloc);

		if (allocMoreThread) {
			allocThread = new Thread(new Runnable() {
				@Override
				public void run() {
					AllocatorStack aStack = null;

					if (isExecutor) {
						for (int sz = MIN_BLOCK_SIZE; sz < MIN_BLOCK_SIZE * 64; sz *= 2) {
							try {
								aStack = alloc(sz);
							} catch (IOException e) {
								e.printStackTrace();
							}
							if (aStack != null) {
								for (int i = 0; i < 100; i++) {
									try {
										alloc(aStack);
									} catch (IOException e) {
										e.printStackTrace();
									}
								}
							}
							logger.info("Pre-Allocate 100 of " + sz/1024 + " KB-buffers");
						}
						allocMore.set(false);
					}

					while (isThreadRunning.get()) {
						try {
							if (allocMore.get()) {
								while (lenBufQ.isEmpty() == false) {
									long t2, t1;
									int len = lenBufQ.pop();

									aStack = alloc(len);

									if (aStack.getCurrentSize() > 99)
										continue;

									t1 = System.nanoTime();
									for (int i = 0; i < 50; i++)
										alloc(aStack);
									t2 = System.nanoTime();
									logger.info("Allocate 50 more " + len/1024 + " KB-buffers in " +
										(t2 - t1) / 1e6 + " ms totalAlloc " + aStack.getTotalAlloc());
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
				}
			});

			allocMore.set(true);
			isThreadRunning.set(true);
			allocThread.start();
		}
	}

	private void alloc(AllocatorStack aStack) throws IOException {
		long t1 = System.nanoTime();

		aStack.addStackBuf(new RdmaBuffer(getPd(), aStack.getLength()));

		long t2 = System.nanoTime();
		logger.trace("Allocate&Pin " + aStack.getLength()/1024 + " KB-buffer in " + (t2 - t1) / 1e6 + " ms");
	}

	private AllocatorStack alloc(int length) throws IOException {
		AllocatorStack aStack = stack_map.get(length);

		if (aStack == null) {
			stack_map.putIfAbsent(length, new AllocatorStack(length));
			aStack = stack_map.get(length);
		}

		this.alloc(aStack);

		return aStack;
	}

	void putRecv(RdmaBuffer buf) throws IOException {
		recvStack.putStackBuf(buf);
	}

	RdmaBuffer getRecv() throws IOException {
		return recvStack.getStackBuf();
	}

	RdmaBuffer get(int length, boolean isAggBlock) throws IOException {
		AllocatorStack newAStack;
		long t1, t2;

		t1 = System.nanoTime();
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
			buf  = newAStack.getStackBuf();

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


		t2 = System.nanoTime();
		if (((t2 - t1) / 1e6) > 1)
			logger.trace("Get " + length/1024 + " KB-buffer in " + (t2 - t1) / 1e6 + " ms");
		*/



		if (newAStack == null) {
			newAStack = alloc(length);
		}
		buf = newAStack.getStackBuf();

		return buf;
	}

	void put(RdmaBuffer buf) {
		int length = buf.getLength();

		AllocatorStack aStack = stack_map.get(length);
		if (aStack == null) {
			logger.error("No aStack found for put()!!!");
			return;
		}

		aStack.putStackBuf(buf);
	}

	public void printMaxNumBufsAlloc() {
		AllocatorStack aStack;

		for (int sz = MIN_BLOCK_SIZE; sz < MIN_BLOCK_SIZE * 64; sz *= 2) {
			aStack = stack_map.get(sz);
			logger.info(" " + sz/1024 + "KB_bufs= " + ((aStack == null) ? 0 : aStack.getTotalAlloc()));
		}
	}

	public IbvPd getPd() {
		return this.pd;
	}

	public ConcurrentHashMap<Integer, AllocatorStack>  getStackMap() {
		return this.stack_map;
	}

	public synchronized void stop() throws InterruptedException {
		if (allocMoreThread) {
			isThreadRunning.set(false);
			synchronized (allocMore) {
				allocMore.notify();
			}
			allocThread.join();
		}

		printMaxNumBufsAlloc();

		for (Integer size: stack_map.keySet()) {
			AllocatorStack astack = stack_map.remove(size);
			astack.emptyStackBufs();
		}
		recvStack.emptyStackBufs();
	}
}
