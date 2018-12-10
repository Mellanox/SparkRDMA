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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.rdma.verbs.SVCRegMr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

public class RdmaMappedFile {
  private static final Logger logger = LoggerFactory.getLogger(RdmaMappedFile.class);
  private static final Method mmap;
  private static final Method unmmap;
  private static final int ACCESS = IbvMr.IBV_ACCESS_REMOTE_READ;
  private static long pageSize;

  private File file;
  private FileChannel fileChannel;

  private final IbvPd ibvPd;

  private final RdmaMapTaskOutput rdmaMapTaskOutput;
  private final RdmaBufferManager rdmaBufferManager;

  public RdmaMapTaskOutput getRdmaMapTaskOutput() { return rdmaMapTaskOutput; }

  private class RdmaFileMapping {
    final IbvMr ibvMr;
    final long address;
    final long mapAddress;
    final long length;
    final long alignedLength;

    RdmaFileMapping(IbvMr ibvMr, long address, long mapAddress, long length, long alignedLength) {
      this.ibvMr = ibvMr;
      this.address = address;
      this.mapAddress = mapAddress;
      this.length = length;
      this.alignedLength = alignedLength;
    }
  }
  private final List<RdmaFileMapping> rdmaFileMappings = new ArrayList<>(1);

  static {
    try {
      mmap = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
      mmap.setAccessible(true);
      unmmap = FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
      unmmap.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      Unsafe unsafe = (Unsafe)f.get(null);
      pageSize =  unsafe.pageSize();
      if ((pageSize == 0) || (pageSize & (pageSize - 1)) != 0) {
        throw new Exception("Page size must be non zero and to be a power of 2");
      }
    } catch (Throwable e) {
      logger.warn("Unable to get operating system page size. Guessing 4096.", e);
      pageSize = 4096;
    }
  }

  public RdmaMappedFile(File file, int chunkSize, long[] partitionLengths,
      RdmaBufferManager rdmaBufferManager) throws IOException, InvocationTargetException,
      IllegalAccessException {
    this.file = file;
    this.ibvPd = rdmaBufferManager.getPd();
    this.rdmaBufferManager = rdmaBufferManager;
    final RandomAccessFile backingFile = new RandomAccessFile(file, "rw");
    this.fileChannel = backingFile.getChannel();

    rdmaMapTaskOutput = new RdmaMapTaskOutput(0, partitionLengths.length - 1);
    mapAndRegister(chunkSize, partitionLengths);

    fileChannel.close();
    fileChannel = null;

    file.deleteOnExit();
  }

  private void mapAndRegister(int chunkSize, long[] partitionLengths) throws IOException,
      InvocationTargetException, IllegalAccessException {
    long offset = 0;
    long curLength = 0;

    for (long length : partitionLengths) {
      if (curLength + length <= chunkSize) {
        curLength += length;
      } else {
        if (curLength > 0) {
          mapAndRegister(offset, curLength);
          offset += curLength;
          curLength = 0;
        }

        if (length >= chunkSize) {
          mapAndRegister(offset, length);
          offset += length;
        } else {
          curLength = length;
        }
      }
    }

    if (curLength > 0) {
      mapAndRegister(offset, curLength);
    }

    int curPartition = 0;
    for (RdmaFileMapping rdmaFileMapping : rdmaFileMappings) {
      curLength = 0;
      while (curLength <= rdmaFileMapping.length && curPartition < partitionLengths.length) {
        curLength += partitionLengths[curPartition];

        if (curLength <= rdmaFileMapping.length) {
          rdmaMapTaskOutput.put(
            curPartition,
            rdmaFileMapping.address + curLength - partitionLengths[curPartition],
            (int)partitionLengths[curPartition],
            rdmaFileMapping.ibvMr.getLkey());
          curPartition++;
        }
      }
    }
  }

  private static long roundUpToNextPageBoundary(long i) {
    return (i + pageSize - 1) & ~(pageSize - 1);
  }

  private void mapAndRegister(long fileOffset, long length) throws IOException,
      InvocationTargetException, IllegalAccessException {
    long distanceFromPageBoundary = fileOffset % pageSize;
    long alignedOffset = fileOffset - distanceFromPageBoundary;
    long alignedLength = roundUpToNextPageBoundary(length + distanceFromPageBoundary);
    long mapAddress = (long)mmap.invoke(fileChannel, 1, alignedOffset, alignedLength);
    long address = mapAddress + distanceFromPageBoundary;

    if (length > Integer.MAX_VALUE) {
      throw new IOException("Registering files larger than " + Integer.MAX_VALUE + "B is not " +
        "supported");
    }

    IbvMr ibvMr = null;
    if (!rdmaBufferManager.useOdp()) {
      SVCRegMr svcRegMr = ibvPd.regMr(address, (int)length, ACCESS).execute();
      ibvMr = svcRegMr.getMr();
      svcRegMr.free();
    } else {
      SVCRegMr svcRegMr = ibvPd.regMr(address, (int)length,
        ACCESS | IbvMr.IBV_ACCESS_ON_DEMAND).execute();
      ibvMr = svcRegMr.getMr();
      svcRegMr.free();
    }

    rdmaFileMappings.add(new RdmaFileMapping(ibvMr, address, mapAddress, length, alignedLength));
  }

  private void unregisterAndUnmap(RdmaFileMapping rdmaFileMapping) throws InvocationTargetException,
      IllegalAccessException, IOException {
    if (rdmaFileMapping.ibvMr != null) {
      rdmaFileMapping.ibvMr.deregMr().execute().free();
    }
    unmmap.invoke(null, rdmaFileMapping.mapAddress, rdmaFileMapping.alignedLength);
    getRdmaMapTaskOutput().getRdmaBuffer().free();
  }

  private void unregisterAndUnmap() throws InvocationTargetException, IllegalAccessException,
      IOException {
    for (RdmaFileMapping rdmaFileMapping : rdmaFileMappings) {
      unregisterAndUnmap(rdmaFileMapping);
    }
    rdmaFileMappings.clear();
  }

  public void dispose() throws IOException, InvocationTargetException, IllegalAccessException {
    unregisterAndUnmap();
    if (fileChannel != null) {
      fileChannel.close();
      fileChannel = null;
    }
    if (file != null) {
      file.delete();
      file = null;
    }
  }

  private ByteBuffer getByteBuffer(long address, int length) throws IOException {
    try {
      return (ByteBuffer)RdmaBuffer.directBufferConstructor.newInstance(address, length);
    } catch (InvocationTargetException ex) {
      throw new IOException("java.nio.DirectByteBuffer: " +
        "InvocationTargetException: " + ex.getTargetException());
    } catch (Exception e) {
      throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
    }
  }

  public ByteBuffer getByteBufferForPartition(int partitionId) throws IOException {
    RdmaBlockLocation rdmaBlockLocation = rdmaMapTaskOutput.getRdmaBlockLocation(partitionId);
    return (rdmaBlockLocation.length() == 0) ? null :
      getByteBuffer(rdmaBlockLocation.address(), rdmaBlockLocation.length());
  }
}
