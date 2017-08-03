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

import com.ibm.disni.rdma.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.concurrent.*;

import java.util.concurrent.atomic.AtomicBoolean;

import scala.concurrent.ExecutionContext;

class RdmaNode {
  private static final Logger logger = LoggerFactory.getLogger(RdmaNode.class);
  private static final int BACKLOG = 128;

  private final RdmaShuffleConf conf;
  private final RdmaCompletionListener receiveListener;
  private final ConcurrentHashMap<InetSocketAddress, RdmaChannel> activeRdmaChannelMap =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<InetSocketAddress, RdmaChannel> passiveRdmaChannelMap =
    new ConcurrentHashMap<>();
  private RdmaBufferManager rdmaBufferManager = null;
  private RdmaCmId listenerRdmaCmId;
  private RdmaEventChannel cmChannel;
  private final AtomicBoolean runThread = new AtomicBoolean(false);
  private Thread listeningThread;
  private IbvPd ibvPd;
  private InetSocketAddress localInetSocketAddress;
  private InetAddress driverInetAddress;

  RdmaNode(String hostName, boolean isExecutor, final RdmaShuffleConf conf,
           final RdmaCompletionListener receiveListener) throws Exception {
    this.receiveListener = receiveListener;
    this.conf = conf;

    try {
      driverInetAddress = InetAddress.getByName(conf.driverHost());

      cmChannel = RdmaEventChannel.createEventChannel();
      if (this.cmChannel == null) {
        throw new IOException("Unable to allocate RDMA Event Channel");
      }

      listenerRdmaCmId = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
      if (this.listenerRdmaCmId == null) {
        throw new IOException("Unable to allocate RDMA CM Id");
      }

      int err = 0;
      int bindPort = isExecutor ? conf.executorPort() : conf.driverPort();
      for (int i = 0; i < conf.portMaxRetries(); i++) {
        err = listenerRdmaCmId.bindAddr(
          new InetSocketAddress(InetAddress.getByName(hostName), bindPort));
        if (err == 0) {
          break;
        }
        logger.info("Failed to bind to port {} on iteration {}", bindPort, i);
        bindPort = bindPort != 0 ? bindPort+1 : 0;
      }
      if (err != 0) {
        throw new IOException("Unable to bind, err: " + err);
      }

      err = listenerRdmaCmId.listen(BACKLOG);
      if (err != 0) {
        throw new IOException("Failed to start listener: " + err);
      }

      localInetSocketAddress = (InetSocketAddress) listenerRdmaCmId.getSource();

      ibvPd = listenerRdmaCmId.getVerbs().allocPd();
      if (ibvPd == null) {
        throw new IOException("Failed to create PD");
      }

      this.rdmaBufferManager = new RdmaBufferManager(ibvPd, isExecutor, conf);
    } catch (IOException e) {
      logger.error("Failed in RdmaNode constructor");
      stop();
      throw e;
    } catch (UnsatisfiedLinkError ule) {
      logger.error("libdisni not found! It must be installed within the java.library.path on each" +
        " Executor and Driver instance");
      throw ule;
    }

    listeningThread = new Thread(() -> {
      logger.info("Starting RdmaNode Listening Server");

      while (runThread.get()) {
        try {
          // Wait for next event
          RdmaCmEvent event = cmChannel.getCmEvent(50);
          if (event == null) {
            continue;
          }

          RdmaCmId cmId = event.getConnIdPriv();
          int eventType = event.getEvent();
          event.ackEvent();

          InetSocketAddress inetSocketAddress = (InetSocketAddress)cmId.getDestination();

          // TODO: Handle reject on redundant connection, and manage mutual connect
          if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST.ordinal()) {
            RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(inetSocketAddress);
            if (rdmaChannel != null) {
              logger.warn("Received a redundant RDMA connection request for " +
                "inetSocketAddress: {}", inetSocketAddress);
              continue;
            }

            boolean isRpc = false;
            if (driverInetAddress.equals(inetSocketAddress.getAddress()) ||
              driverInetAddress.equals(localInetSocketAddress.getAddress())) {
              isRpc = true;
            }

            rdmaChannel = new RdmaChannel(
              conf,
              rdmaBufferManager,
              false,
              true,
              receiveListener,
              cmId,
              isRpc,
              true);
            if (passiveRdmaChannelMap.putIfAbsent(inetSocketAddress, rdmaChannel) != null) {
              logger.warn("Race creating the RDMA Channel for inetSocketAddress: {}",
                inetSocketAddress);
              rdmaChannel.stop();
              continue;
            }

            rdmaChannel.accept();
          } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal()) {
            RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(inetSocketAddress);
            if (rdmaChannel == null) {
              logger.warn("Received Established Event for inetSocketAddress not in the " +
                "passiveRdmaChannelMap, {}", inetSocketAddress);
              continue;
            }

            rdmaChannel.finalizeConnection();
          } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
            RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
            if (rdmaChannel == null) {
              logger.warn("Received Disconnect Event for inetSocketAddress not in the " +
                "passiveRdmaChannelMap, {}", inetSocketAddress);
              continue;
            }

            rdmaChannel.stop();
          } else {
            logger.info("Unexpected CM Event {}", eventType);
          }
        } catch (Exception e) {
          // TODO: Improve handling of exceptions
          e.printStackTrace();
        }
      }
      logger.info("Exiting RdmaNode Listening Server");
    }, "RdmaNode connection listening thread");

    runThread.set(true);
    listeningThread.start();
  }

  public RdmaBufferManager getRdmaBufferManager() { return rdmaBufferManager; }

  public RdmaChannel getRdmaChannel(InetSocketAddress remoteAddr)
      throws IOException, InterruptedException {
    RdmaChannel rdmaChannel;

    while (true) {
      rdmaChannel = activeRdmaChannelMap.get(remoteAddr);
      if (rdmaChannel == null) {
        rdmaChannel = new RdmaChannel(
          conf,
          rdmaBufferManager,
          true,
          false,
          receiveListener,
          false,
          false);

        RdmaChannel actualRdmaChannel = activeRdmaChannelMap.putIfAbsent(remoteAddr, rdmaChannel);
        if (actualRdmaChannel != null) {
          rdmaChannel = actualRdmaChannel;
        } else {
          try {
            long startTime = System.nanoTime();
            rdmaChannel.connect(remoteAddr);
            logger.info("Established connection to " + remoteAddr + " in " +
              (System.nanoTime() - startTime) / 1000000 + " ms");
          } catch (IOException e) {
            logger.error("connect failed");
            activeRdmaChannelMap.remove(remoteAddr, rdmaChannel);
            rdmaChannel.stop();
            throw e;
          }
        }
      }

      // TODO: Need to handle failures, add timeout, handle dead connections
      // TODO: Limit number of iterations in while loop
      if (!rdmaChannel.isConnected()) {
        rdmaChannel.waitForActiveConnection();
      }

      if (rdmaChannel.isConnected()) {
        break;
      }
    }

    assert(rdmaChannel.isConnected());
    return rdmaChannel;
  }

  private FutureTask<Void> createFutureChannelStopTask(final RdmaChannel rdmaChannel) {
    FutureTask<Void> futureTask = new FutureTask<>(() -> {
      try {
        rdmaChannel.stop();
      } catch (InterruptedException | IOException e) {
        logger.warn("Exception caught while stopping an RdmaChannel", e);
      }
    }, null);

    // TODO: Use our own ExecutorService in Java
    ExecutionContext.Implicits$.MODULE$.global().execute(futureTask);
    return futureTask;
  }

  void stop() throws Exception {
    // Spawn simultaneous disconnect tasks to speed up tear-down
    LinkedList<FutureTask<Void>> futureTaskList = new LinkedList<>();
    for (InetSocketAddress inetSocketAddress: activeRdmaChannelMap.keySet()) {
      final RdmaChannel rdmaChannel = activeRdmaChannelMap.remove(inetSocketAddress);
      futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
    }

    // Wait for all of the channels to disconnect
    for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

    if (runThread.getAndSet(false)) { listeningThread.join(); }

    // Spawn simultaneous disconnect tasks to speed up tear-down
    futureTaskList = new LinkedList<>();
    for (InetSocketAddress inetSocketAddress: passiveRdmaChannelMap.keySet()) {
      final RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
      futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
    }

    // Wait for all of the channels to disconnect
    for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

    if (rdmaBufferManager != null) { rdmaBufferManager.stop(); }
    if (ibvPd != null) { ibvPd.deallocPd(); }
    if (listenerRdmaCmId != null) { listenerRdmaCmId.destroyId(); }
    if (cmChannel != null) { cmChannel.destroyEventChannel(); }
  }

  public InetSocketAddress getLocalInetSocketAddress() { return localInetSocketAddress; }
}