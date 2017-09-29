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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RdmaChannel {
  private static final Logger logger = LoggerFactory.getLogger(RdmaChannel.class);
  private static final int MAX_ACK_COUNT = 4;
  private static final int POLL_CQ_LIST_SIZE = 16;
  private static final int ZERO_SIZED_RECV_WR_LIST_SIZE = 16;

  private final RdmaCompletionListener receiveListener;
  private final RdmaBufferManager rdmaBufferManager;
  private IbvCompChannel compChannel = null;
  private RdmaEventChannel eventChannel = null;
  private final int rdmaCmEventTimeout;
  private final int teardownListenTimeout;
  private final int resolvePathTimeout;
  private RdmaCmId cmId = null;
  private IbvCQ cq = null;
  private IbvQP qp = null;
  private boolean qpIsErr = false;
  private final AtomicBoolean isStopped = new AtomicBoolean(false);

  private Semaphore sendBudgetSemaphore;
  private final ConcurrentLinkedDeque<LinkedList<IbvSendWR>> sendWrQueue =
    new ConcurrentLinkedDeque<>();

  private class PostRecvWr {
    final IbvRecvWR ibvRecvWR;
    final RdmaBuffer rdmaBuf;
    final ByteBuffer buf;

    PostRecvWr(IbvRecvWR ibvRecvWR, RdmaBuffer rdmaBuf) throws IOException {
      this.ibvRecvWR = ibvRecvWR;
      this.rdmaBuf = rdmaBuf;
      this.buf = rdmaBuf.getByteBuffer();
    }
  }
  private PostRecvWr[] postRecvWrArray = null;

  private int ackCounter = 0;

  private int sendDepth = 0;
  private int recvDepth = 0;
  private int recvWrSize;
  private LinkedList<IbvRecvWR> zeroSizeRecvWrList;

  private boolean isWarnedOnSendOverSubscription = false;

  private final int cpuVector;

  private final boolean isPassive;

  private SVCReqNotify reqNotifyCall;
  private SVCPollCq svcPollCq;
  private IbvWC[] ibvWCs;

  private RdmaThread rdmaThread = null;

  private final static int STATE_IDLE         = 0;
  private final static int STATE_CONNECTING   = 1;
  private final static int STATE_CONNECTED    = 2;
  private final static int STATE_ERROR        = 4;

  private final AtomicInteger connectState = new AtomicInteger(STATE_IDLE);

  private class CompletionInfo {
    final RdmaCompletionListener listener;
    final int sendPermitsToReclaim;

    CompletionInfo(RdmaCompletionListener listener, int sendPermitsToReclaim) {
      this.listener = listener;
      this.sendPermitsToReclaim = sendPermitsToReclaim;
    }
  }
  private final ConcurrentHashMap<Integer, CompletionInfo> completionInfoMap =
    new ConcurrentHashMap<>();
  private final AtomicInteger completionInfoIndex = new AtomicInteger(1); // 0 reserved for null

  RdmaChannel(
      RdmaShuffleConf conf,
      RdmaBufferManager rdmaBufferManager,
      boolean needSends,
      boolean needRecvs,
      RdmaCompletionListener receiveListener,
      RdmaCmId cmId,
      boolean isRpc,
      boolean isPassive,
      int cpuVector) {
    this(conf, rdmaBufferManager, needSends, needRecvs, receiveListener, isRpc, isPassive,
      cpuVector);
    this.cmId = cmId;
  }

  RdmaChannel(
      RdmaShuffleConf conf,
      RdmaBufferManager rdmaBufferManager,
      boolean needSends,
      boolean needRecvs,
      RdmaCompletionListener receiveListener,
      boolean isRpc,
      boolean isPassive,
      int cpuVector) {
    this.receiveListener = receiveListener;
    this.rdmaBufferManager = rdmaBufferManager;
    this.isPassive = isPassive;
    this.cpuVector = cpuVector;

    if (needSends) {
      this.sendDepth = conf.sendQueueDepth();
      // We don't care about the fairness of the semaphore, since it is implied from wrapping logic
      this.sendBudgetSemaphore = new Semaphore(sendDepth, false);
    }

    if (needRecvs) {
      this.recvDepth = conf.recvQueueDepth();
      this.postRecvWrArray = new PostRecvWr[recvDepth];
      this.recvWrSize = isRpc ? conf.recvWrSize() : 0;
    }

    this.rdmaCmEventTimeout = conf.rdmaCmEventTimeout();
    this.teardownListenTimeout = conf.teardownListenTimeout();
    this.resolvePathTimeout = conf.resolvePathTimeout();
  }

  private int putCompletionInfo(CompletionInfo completionInfo) {
    int index = completionInfoIndex.getAndIncrement();
    CompletionInfo retCompletionInfo = completionInfoMap.put(index, completionInfo);
    if (retCompletionInfo != null) {
      throw new RuntimeException("Overflow of CompletionInfos");
    }
    return index;
  }

  private CompletionInfo removeCompletionInfo(int index) {
    return completionInfoMap.remove(index);
  }

  private void setupCommon() throws IOException {
    IbvContext ibvContext = cmId.getVerbs();
    if (ibvContext == null) {
      throw new IOException("Failed to retrieve IbvContext");
    }

    compChannel = ibvContext.createCompChannel();
    if (compChannel == null) {
      throw new IOException("createCompChannel() failed");
    }

    cq = ibvContext.createCQ(compChannel, sendDepth + recvDepth, cpuVector);
    if (cq == null) {
      throw new IOException("createCQ() failed");
    }

    reqNotifyCall = cq.reqNotification(false);
    reqNotifyCall.execute();

    ibvWCs = new IbvWC[POLL_CQ_LIST_SIZE];
    for (int i = 0; i < POLL_CQ_LIST_SIZE; i++) {
      ibvWCs[i] = new IbvWC();
    }
    svcPollCq = cq.poll(ibvWCs, POLL_CQ_LIST_SIZE);

    IbvQPInitAttr attr = new IbvQPInitAttr();
    attr.setQp_type(IbvQP.IBV_QPT_RC);
    attr.setSend_cq(cq);
    attr.setRecv_cq(cq);
    attr.cap().setMax_recv_sge(1);
    attr.cap().setMax_recv_wr(recvDepth);
    attr.cap().setMax_send_sge(1);
    attr.cap().setMax_send_wr(sendDepth);

    qp = cmId.createQP(rdmaBufferManager.getPd(), attr);
    if (qp == null) {
      throw new IOException("createQP() failed");
    }

    if (recvWrSize == 0) {
      initZeroSizeRecvs();
    } else {
      initRecvs();
    }

    rdmaThread = new RdmaThread(this, cpuVector);
    rdmaThread.start();
  }

  void connect(InetSocketAddress socketAddress) throws IOException {
    eventChannel = RdmaEventChannel.createEventChannel();
    if (eventChannel == null) {
      throw new IOException("createEventChannel() failed");
    }

    // Create an active connect cm id
    cmId = eventChannel.createId(RdmaCm.RDMA_PS_TCP);
    if (cmId == null) {
      throw new IOException("createId() failed");
    }

    // Resolve the addr
    connectState.set(STATE_CONNECTING);
    int err = cmId.resolveAddr(null, socketAddress, resolvePathTimeout);
    if (err != 0) {
      throw new IOException("resolveAddr() failed: " + err);
    }

    processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_ADDR_RESOLVED.ordinal(),
      rdmaCmEventTimeout);

    // Resolve the route
    err = cmId.resolveRoute(resolvePathTimeout);
    if (err != 0) {
      throw new IOException("resolveRoute() failed: " + err);
    }

    processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_ROUTE_RESOLVED.ordinal(),
      rdmaCmEventTimeout);

    setupCommon();

    RdmaConnParam connParams = new RdmaConnParam();
    // TODO: current disni code does not support setting these
    // connParams.setInitiator_depth((byte) 16);
    // connParams.setResponder_resources((byte) 16);
    // retry infinite
    connParams.setRetry_count((byte) 7);
    connParams.setRnr_retry_count((byte) 7);

    err = cmId.connect(connParams);
    if (err != 0) {
      connectState.set(STATE_ERROR);
      throw new IOException("connect() failed");
    }

    processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal(),
      rdmaCmEventTimeout);
    connectState.set(STATE_CONNECTED);
  }

  void accept() throws IOException {
    RdmaConnParam connParams = new RdmaConnParam();

    setupCommon();

    // TODO: current disni code does not support setting these
    //connParams.setInitiator_depth((byte) 16);
    //connParams.setResponder_resources((byte) 16);
    // retry infinite
    connParams.setRetry_count((byte) 7);
    connParams.setRnr_retry_count((byte) 7);

    connectState.set(STATE_CONNECTING);

    int err = cmId.accept(connParams);
    if (err != 0) {
      connectState.set(STATE_ERROR);
      throw new IOException("accept() failed");
    }
  }

  void finalizeConnection() {
    connectState.set(STATE_CONNECTED);
    synchronized (connectState) { connectState.notifyAll(); }
  }

  private void processRdmaCmEvent(int expectedEvent, int timeout) throws IOException {
    RdmaCmEvent event = eventChannel.getCmEvent(timeout);
    if (event == null) {
      connectState.set(STATE_ERROR);
      throw new IOException("getCmEvent() failed");
    }

    int eventType = event.getEvent();
    event.ackEvent();

    if (eventType != expectedEvent) {
      connectState.set(STATE_ERROR);
      throw new IOException("Received CM event: " + eventType + " but expected: " + expectedEvent);
    }
  }

  void waitForActiveConnection() {
    synchronized (connectState) {
      try {
        connectState.wait(100);
      } catch (InterruptedException ignored) {}
    }
  }

  private void rdmaPostWRList(LinkedList<IbvSendWR> sendWRList) throws IOException {
    if (qpIsErr || isStopped.get()) {
      throw new IOException("QP is in error state, can't post new requests");
    }

    SVCPostSend sendSVCPostSend = qp.postSend(sendWRList, null);
    sendSVCPostSend.execute();
    sendSVCPostSend.free();
  }

  private void rdmaPostWRListInQueue(LinkedList<IbvSendWR> sendWRList) throws IOException {
    if (qpIsErr || isStopped.get()) {
      throw new IOException("QP is in error state, can't post new requests");
    }

    if (sendBudgetSemaphore.tryAcquire(sendWRList.size())) {
      // Ordering is lost here since if there are credits avail they will be immediately utilized
      // without fairness. We don't care about fairness, since Spark doesn't expect the requests to
      // complete in a particular order
      rdmaPostWRList(sendWRList);
    } else {
      if (!isWarnedOnSendOverSubscription) {
        isWarnedOnSendOverSubscription = true;
        logger.warn("RDMA channel " + this + " oversubscription detected. RDMA" +
          " send queue depth is too small. To improve performance, please set" +
          " set spark.shuffle.io.rdmaSendDepth to a higher value (current depth: " + sendDepth);
      }
      sendWrQueue.add(sendWRList);

      // Try again, in case it is the only WR in the queue and there are no pending sends
      if (sendBudgetSemaphore.tryAcquire(sendWRList.size())) {
        if (sendWrQueue.remove(sendWRList)) {
          rdmaPostWRList(sendWRList);
        } else {
          sendBudgetSemaphore.release(sendWRList.size());
        }
      }
    }
  }

  void rdmaReadInQueue(RdmaCompletionListener listener, long localAddress, int lKey,
      int sizes[], long[] remoteAddresses, int[] rKeys) throws IOException {
    long offset = 0;
    LinkedList<IbvSendWR> readWRList = new LinkedList<>();
    for (int i = 0; i < remoteAddresses.length; i++) {
      IbvSge readSge = new IbvSge();
      readSge.setAddr(localAddress + offset);
      readSge.setLength(sizes[i]);
      readSge.setLkey(lKey);
      offset += sizes[i];

      LinkedList<IbvSge> readSgeList = new LinkedList<>();
      readSgeList.add(readSge);

      IbvSendWR readWr = new IbvSendWR();
      readWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_READ.ordinal());
      readWr.setSg_list(readSgeList);
      readWr.getRdma().setRemote_addr(remoteAddresses[i]);
      readWr.getRdma().setRkey(rKeys[i]);

      readWRList.add(readWr);
    }

    readWRList.getLast().setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    int completionInfoId = putCompletionInfo(new CompletionInfo(listener, remoteAddresses.length));
    readWRList.getLast().setWr_id(completionInfoId);

    try {
      rdmaPostWRListInQueue(readWRList);
    } catch (Exception e) {
      removeCompletionInfo(completionInfoId);
      throw e;
    }
  }

  void rdmaSendInQueue(RdmaCompletionListener listener, long[] localAddresses, int[] lKeys,
      int[] sizes) throws IOException {
    LinkedList<IbvSendWR> sendWRList = new LinkedList<>();
    for (int i = 0; i < localAddresses.length; i++) {
      IbvSge sendSge = new IbvSge();
      sendSge.setAddr(localAddresses[i]);
      sendSge.setLength(sizes[i]);
      sendSge.setLkey(lKeys[i]);

      LinkedList<IbvSge> sendSgeList = new LinkedList<>();
      sendSgeList.add(sendSge);

      IbvSendWR sendWr = new IbvSendWR();
      sendWr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());
      sendWr.setSg_list(sendSgeList);

      sendWRList.add(sendWr);
    }

    sendWRList.getLast().setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    int completionInfoId = putCompletionInfo(new CompletionInfo(listener, localAddresses.length));
    sendWRList.getLast().setWr_id(completionInfoId);

    try {
      rdmaPostWRListInQueue(sendWRList);
    } catch (Exception e) {
      removeCompletionInfo(completionInfoId);
      throw e;
    }
  }

  private void initZeroSizeRecvs() throws IOException {
    if (recvDepth == 0) {
      return;
    }

    IbvRecvWR wr = new IbvRecvWR();
    wr.setWr_id(recvDepth);
    wr.setNum_sge(0);

    zeroSizeRecvWrList = new LinkedList<>();
    for (int i = 0; i < ZERO_SIZED_RECV_WR_LIST_SIZE; i++) {
      zeroSizeRecvWrList.add(wr);
    }

    postZeroSizeRecvWrs(recvDepth);
  }

  private void postZeroSizeRecvWrs(int count) throws IOException {
    if (qpIsErr || isStopped.get() || recvDepth == 0) {
      return;
    }

    int cPosted = 0;
    List<IbvRecvWR> actualRecvWrList = zeroSizeRecvWrList;
    while (cPosted < count) {
      int cCurrentPost = ZERO_SIZED_RECV_WR_LIST_SIZE;
      if (count - cPosted < ZERO_SIZED_RECV_WR_LIST_SIZE) {
        actualRecvWrList = zeroSizeRecvWrList.subList(0, count - cPosted);
        cCurrentPost = count - cPosted;
      }

      SVCPostRecv svcPostRecv = qp.postRecv(actualRecvWrList, null);
      svcPostRecv.execute();
      svcPostRecv.free();

      cPosted += cCurrentPost;
    }
  }

  private void postRecvWrs(int startIndex, int count) throws IOException {
    if (qpIsErr || isStopped.get() || recvDepth == 0) {
      return;
    }

    LinkedList<IbvRecvWR> recvWrList = new LinkedList<>();
    for (int i = startIndex; i < startIndex + count; i++) {
      postRecvWrArray[i % recvDepth].buf.clear();
      postRecvWrArray[i % recvDepth].buf.limit(recvWrSize);
      recvWrList.add(postRecvWrArray[i % recvDepth].ibvRecvWR);
    }

    SVCPostRecv svcPostRecv = qp.postRecv(recvWrList, null);
    svcPostRecv.execute();
    svcPostRecv.free();
  }

  private void initRecvs() throws IOException {
    if (qpIsErr || isStopped.get() || recvDepth == 0) {
      return;
    }

    LinkedList<IbvRecvWR> recvWrList = new LinkedList<>();
    for (int i = 0; i < recvDepth; i++) {
      RdmaBuffer rdmaBuffer = rdmaBufferManager.get(recvWrSize);

      IbvSge sge = new IbvSge();
      sge.setAddr(rdmaBuffer.getAddress());
      sge.setLength(rdmaBuffer.getLength());
      sge.setLkey(rdmaBuffer.getLkey());

      LinkedList<IbvSge> sgeList = new LinkedList<>();
      sgeList.add(sge);

      IbvRecvWR wr = new IbvRecvWR();
      wr.setWr_id(i);
      wr.setSg_list(sgeList);

      postRecvWrArray[i] = new PostRecvWr(wr, rdmaBuffer);

      recvWrList.add(wr);
    }

    SVCPostRecv svcPostRecv = qp.postRecv(recvWrList, null);
    svcPostRecv.execute();
    svcPostRecv.free();
  }

  private void exhaustCq() throws IOException {
    int reclaimedSendPermits = 0;
    int reclaimedRecvWrs = 0;
    int firstRecvWrIndex = -1;

    while (true) {
      int res = svcPollCq.execute().getPolls();
      if (res < 0) {
        logger.error("PollCQ failed executing with res: " + res);
        break;
      } else if (res > 0) {
        for (int i = 0; i < res; i++) {
          boolean wcSuccess = ibvWCs[i].getStatus() == IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal();
          if (!wcSuccess && !qpIsErr) {
            qpIsErr = true;
            logger.error("Completion with error: " + ibvWCs[i].getStatus());
          }

          if (ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.getOpcode() ||
              ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RDMA_READ.getOpcode()) {
            int completionInfoId = (int)ibvWCs[i].getWr_id();
            CompletionInfo completionInfo = removeCompletionInfo(completionInfoId);
            if (completionInfo != null) {
              if (wcSuccess) {
                completionInfo.listener.onSuccess(null);
              } else {
                completionInfo.listener.onFailure(
                  new IOException("RDMA Send/Read WR completed with error: " +
                    ibvWCs[i].getStatus()));
              }

              reclaimedSendPermits += completionInfo.sendPermitsToReclaim;
            } else if (wcSuccess) {
              // Ignore the case of error, as the listener will be invoked by the last WC
              logger.warn("Couldn't find CompletionInfo with index: " + completionInfoId);
            }
          } else if (ibvWCs[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {
            int recvWrId = (int)ibvWCs[i].getWr_id();
            if (firstRecvWrIndex == -1) {
              firstRecvWrIndex = recvWrId;
            }

            if (wcSuccess) {
              if (recvWrSize > 0) {
                receiveListener.onSuccess(postRecvWrArray[recvWrId].buf);
              } else {
                receiveListener.onSuccess(null);
              }
            } else {
              receiveListener.onFailure(
                new IOException("RDMA Receive WR completed with error: " + ibvWCs[i].getStatus()));
            }

            reclaimedRecvWrs += 1;
          } else {
            logger.error("Unexpected opcode in PollCQ: " + ibvWCs[i].getOpcode());
          }
        }
      } else {
        break;
      }
    }

    if (qpIsErr) {
      connectState.set(STATE_ERROR);
      throw new IOException("QP entered ERROR state");
    }

    if (reclaimedRecvWrs > 0) {
      if (recvWrSize > 0) {
        postRecvWrs(firstRecvWrIndex, reclaimedRecvWrs);
      } else {
        postZeroSizeRecvWrs(reclaimedRecvWrs);
      }
    }

    // Drain pending sends queue
    while (sendDepth > 0 && !isStopped.get() && !qpIsErr) {
      LinkedList<IbvSendWR> sendWRList = sendWrQueue.poll();
      if (sendWRList != null) {
        // If there are not enough available permits from
        // this run AND from the semaphore, then it means that there are
        // more completions coming and they will exhaust the queue later
        if (sendWRList.size() > reclaimedSendPermits) {
          if (!sendBudgetSemaphore.tryAcquire(sendWRList.size() - reclaimedSendPermits)) {
            sendWrQueue.push(sendWRList);
            sendBudgetSemaphore.release(reclaimedSendPermits);
            break;
          } else {
            reclaimedSendPermits = 0;
          }
        } else {
          reclaimedSendPermits -= sendWRList.size();
        }

        try {
          rdmaPostWRList(sendWRList);
        } catch (IOException e) {
          qpIsErr = true;

          // reclaim the credit and put sendWRList back to the queue
          // however, the channel/QP is already broken and more actions
          // needed to be taken to recover
          reclaimedSendPermits += sendWRList.size();
          sendWrQueue.push(sendWRList);
          sendBudgetSemaphore.release(reclaimedSendPermits);
          break;
        }
      } else {
        sendBudgetSemaphore.release(reclaimedSendPermits);
        break;
      }
    }
  }

  boolean processCompletions() throws IOException {
    // Disni's API uses a CQ here, which is wrong
    boolean success = compChannel.getCqEvent(cq, 50);
    if (success) {
      ackCounter++;
      if (ackCounter == MAX_ACK_COUNT) {
        cq.ackEvents(ackCounter);
        ackCounter = 0;
      }

      if (!isStopped.get()) {
        reqNotifyCall.execute();
      }

      exhaustCq();

      return true;
    } else if (isStopped.get() && ackCounter > 0) {
      cq.ackEvents(ackCounter);
      ackCounter = 0;
    }

    return false;
  }

  void stop() throws InterruptedException, IOException {
    if (!isStopped.getAndSet(true)) {
      logger.info("Stopping RdmaChannel " + this);

      if (rdmaThread != null) rdmaThread.stop();

      // Fail pending completionInfos
      for (Integer completionInfoId: completionInfoMap.keySet()) {
        final CompletionInfo completionInfo = completionInfoMap.remove(completionInfoId);
        if (completionInfo != null) {
          completionInfo.listener.onFailure(
            new IOException("RDMA Send/Read WR revoked since QP was removed"));
        }
      }

      if (cmId != null) {
        int ret = cmId.disconnect();
        if (ret != 0) {
          logger.error("disconnect failed with errno: " + ret);
        } else if (!isPassive) {
          try {
            processRdmaCmEvent(RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal(),
              teardownListenTimeout);
          } catch (IOException e) {
            logger.warn("Failed to get RDMA_CM_EVENT_DISCONNECTED");
          }
        }

        if (qp != null) {
          ret = cmId.destroyQP();
          if (ret != 0) {
            logger.error("destroyQP failed with errno: " + ret);
          }
        }
      }

      if (recvWrSize > 0) {
        for (int i = 0; i < recvDepth; i++) {
          if (postRecvWrArray[i] != null) {
            rdmaBufferManager.put(postRecvWrArray[i].rdmaBuf);
          }
        }
      }

      if (reqNotifyCall != null) {
        reqNotifyCall.free();
      }

      if (svcPollCq != null) {
        svcPollCq.free();
      }

      if (cq != null) {
        int ret = cq.destroyCQ();
        if (ret != 0) {
          logger.error("destroyCQ failed with errno: " + ret);
        }
      }

      if (cmId != null) {
        int ret = cmId.destroyId();
        if (ret != 0) {
          logger.error("destroyId failed with errno: " + ret);
        }
      }

      if (compChannel != null) {
        int ret = compChannel.destroyCompChannel();
        if (ret != 0) {
          logger.error("destroyCompChannel failed with errno: " + ret);
        }
      }

      if (eventChannel != null) {
        int ret = eventChannel.destroyEventChannel();
        if (ret != 0) {
          logger.error("destroyEventChannel failed with errno: " + ret);
        }
      }
    }
  }

  boolean isConnected() { return connectState.get() == STATE_CONNECTED; }
  boolean isError() { return connectState.get() == STATE_ERROR; }
}
