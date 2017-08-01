package org.apache.spark.shuffle.rdma;

import com.ibm.disni.rdma.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.concurrent.*;
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

import java.util.concurrent.atomic.AtomicBoolean;

import scala.concurrent.ExecutionContext;

public class RdmaNode {
  private static final Logger logger = LoggerFactory.getLogger(RdmaNode.class);
  private static final int BACKLOG = 128;
  private final RdmaShuffleConf conf;
  private final RdmaCompletionListener receiveListener;
  private ConcurrentHashMap<InetSocketAddress, RdmaChannel> activeRdmaChannelMap = new ConcurrentHashMap<>();
  private ConcurrentHashMap<InetSocketAddress, RdmaChannel> passiveRdmaChannelMap = new ConcurrentHashMap<>();
  private RdmaBufferManager rdmaBufferManager = null;
  private RdmaCmId listenCmId;
  private RdmaEventChannel cmChannel;
  private AtomicBoolean runThread = new AtomicBoolean(false);
  private Thread listeningThread;
  private IbvPd pd;
  private InetSocketAddress localAddress;
  private InetAddress driverInetAddress;

  public RdmaNode(String hostName, boolean isExecutor, final RdmaShuffleConf conf,
      final RdmaCompletionListener receiveListener) throws Exception {
    this.receiveListener = receiveListener;
    this.conf = conf;

    try {
      driverInetAddress = InetAddress.getByName(conf.driverHost());

      cmChannel = RdmaEventChannel.createEventChannel();
      if (this.cmChannel == null) {
        throw new IOException("Unable to allocate RDMA Event Channel");
      }

      listenCmId = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
      if (this.listenCmId == null) {
        throw new IOException("Unable to allocate RDMA CM Id");
      }

      int err = 0;
      int bindPort = isExecutor ? conf.executorPort() : conf.driverPort();
      for (int i = 0; i < conf.portMaxRetries(); i++) {
        err = listenCmId.bindAddr(new InetSocketAddress(InetAddress.getByName(hostName), bindPort));
        if (err == 0) {
          break;
        }
        logger.info("Failed to bind to port {} on iteration {}", bindPort, i);
        bindPort = bindPort != 0 ? bindPort+1 : 0;
      }

      if (err != 0) {
        throw new IOException("Unable to bind, err: " + err);
      }

      err = listenCmId.listen(BACKLOG);
      if (err != 0) {
        throw new IOException("Failed to start listener: " + err);
      }

      localAddress = (InetSocketAddress)listenCmId.getSource();

      pd = listenCmId.getVerbs().allocPd();
      if (pd == null) {
        throw new IOException("Failed to create PD");
      }
    } catch (IOException e) {
      logger.error("Failed in RdmaNode constructor");
      stop();
      throw e;
    } catch (UnsatisfiedLinkError ule) {
      logger.error("libdisni not found! It must be installed within the java.library.path on each Executor and Driver instance");
      throw ule;
    }

    rdmaBufferManager = new RdmaBufferManager(pd, isExecutor, conf);

    listeningThread = new Thread(new Runnable() {
      @Override
      public void run() {
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

            // TODO: must implement reject on redundant connection, and manage mutual connect
            InetSocketAddress iA = (InetSocketAddress)cmId.getDestination();
            if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST.ordinal()) {
              if (passiveRdmaChannelMap.get(iA) != null) {
                logger.error("Received redundant RDMA connection request for iA: {}", iA);
                continue;
              }

              boolean isRpc = false;
              if (driverInetAddress.equals(iA.getAddress()) || driverInetAddress.equals(localAddress.getAddress())) {
                isRpc = true;
              }
              RdmaChannel rdmaChannel = new RdmaChannel(conf, rdmaBufferManager, false, true, receiveListener, cmId, isRpc, true);
              if (passiveRdmaChannelMap.putIfAbsent(iA, rdmaChannel) != null) {
                logger.error("Race creating the RDMA Channel for iA: {}", iA);
                rdmaChannel.stop();
                continue;
              }

              rdmaChannel.rdmaAccept();
            } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal()) {
              RdmaChannel rdmaChannel = passiveRdmaChannelMap.get(iA);
              if (rdmaChannel == null) {
                logger.error("Received Established Event for iA not in the rdmaChannelMap, {}", iA);
                continue;
              }

              rdmaChannel.finalizeConnection();
            } else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
              RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(iA);
              if (rdmaChannel == null) {
                logger.error("Received Disconnect Event for iA not in the rdmaChannelMap, {}", iA);
                continue;
              }
              rdmaChannel.stop();
            } else {
                logger.info("Unhandled CM Event {}", eventType);
                continue;
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        logger.info("Exiting RdmaNode Listening Server");
      }
    });

    runThread.set(true);
    listeningThread.start();
  }

  public RdmaBufferManager getRdmaBufferManager() { return rdmaBufferManager; }

  public RdmaChannel getRdmaChannel(InetSocketAddress remoteAddr) throws IOException, InterruptedException {
    RdmaChannel rdmaChannel;

    while (true) {
      rdmaChannel = activeRdmaChannelMap.get(remoteAddr);
      if (rdmaChannel == null) {
        rdmaChannel = new RdmaChannel(conf, rdmaBufferManager, true, false, receiveListener, false, false);
        RdmaChannel actualRdmaChannel = activeRdmaChannelMap.putIfAbsent(remoteAddr, rdmaChannel);
        if (actualRdmaChannel != null) {
          rdmaChannel = actualRdmaChannel;
        } else {
          try {
            long startTime = System.nanoTime();
            rdmaChannel.rdmaConnect(remoteAddr);
            logger.info("Established connection to " + remoteAddr + ", took " +
              (System.nanoTime() - startTime) / 1000000 + " ms");
          } catch (IOException e) {
            logger.error("rdmaConnect failed");
            activeRdmaChannelMap.remove(remoteAddr, rdmaChannel);
            rdmaChannel.stop();
            throw e;
          }
        }
      }

      // TODO: Need to handle failures, add timeout, handle dead connections
      // TODO: timeout is not good since we have an issue with multiple connections
      if (rdmaChannel.isConnecting() || rdmaChannel.isIdle()) {
        rdmaChannel.waitForActiveConnection(100);
      }

      if (rdmaChannel.isConnected()) {
        break;
      }
    }

    assert (rdmaChannel.isConnected());
    return rdmaChannel;
  }

  private FutureTask<Void> createFutureChannelStopTask(final RdmaChannel rdmaChannel) {
    FutureTask<Void> futureTask = new FutureTask<>(new Runnable() {
      public void run() {
        try {
          rdmaChannel.stop();
        } catch (InterruptedException | IOException e) {
          logger.warn("Exception caught while stopping an RdmaChannel", e);
        }
      }
    }, null);
    // TODO: We should use our own ExectorService in Java
    ExecutionContext.Implicits$.MODULE$.global().execute(futureTask);

    return futureTask;
  }

  public void stop() throws Exception {
    LinkedList<FutureTask<Void>> futureTaskList = new LinkedList<>();
    for (InetSocketAddress inetSocketAddress: activeRdmaChannelMap.keySet()) {
      final RdmaChannel rdmaChannel = activeRdmaChannelMap.remove(inetSocketAddress);
      futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
    }

    for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

    if (runThread.get()) {
      runThread.set(false);
      listeningThread.join();
    }

    futureTaskList = new LinkedList<>();
    for (InetSocketAddress inetSocketAddress: passiveRdmaChannelMap.keySet()) {
      final RdmaChannel rdmaChannel = passiveRdmaChannelMap.remove(inetSocketAddress);
      futureTaskList.add(createFutureChannelStopTask(rdmaChannel));
    }

    for (FutureTask<Void> futureTask: futureTaskList) { futureTask.get(); }

    if (rdmaBufferManager != null) { rdmaBufferManager.stop(); }
    if (pd != null) { pd.deallocPd(); }
    if (listenCmId != null) { listenCmId.destroyId(); }
    if (cmChannel != null) { cmChannel.destroyEventChannel(); }
  }

  public InetSocketAddress getLocalAddress() { return localAddress; }
}