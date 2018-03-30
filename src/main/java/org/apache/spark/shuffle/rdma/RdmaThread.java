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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import com.ibm.disni.util.NativeAffinity;

class RdmaThread implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(RdmaThread.class);

  private final RdmaChannel rdmaChannel;
  private final int cpuVector;
  private final Thread thread = new Thread(this, "RdmaChannel CQ processing thread");
  private final AtomicBoolean runThread  = new AtomicBoolean(false);

  RdmaThread(RdmaChannel rdmaChannel, int cpuVector) {
    this.rdmaChannel = rdmaChannel;
    this.cpuVector = cpuVector;
    thread.setDaemon(true);
  }

  synchronized void start() {
    runThread.set(true);
    thread.start();
  }

  public void run() {
    long affinity = 1L << cpuVector;
    NativeAffinity.setAffinity(affinity);

    boolean isStillProcessing = false;
    while (runThread.get() || isStillProcessing) {
      try {
        isStillProcessing = rdmaChannel.processCompletions();
      } catch (IOException ioe) {
        logger.error("Exception in RdmaThread, aborting: " + ioe);
        runThread.getAndSet(false);
        isStillProcessing = false;
      }
    }
  }

  synchronized void stop() throws InterruptedException {
    if (runThread.getAndSet(false)) { thread.join(); }
  }
}
