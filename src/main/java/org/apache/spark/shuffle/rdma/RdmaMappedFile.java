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
import sun.nio.ch.FileChannelImpl;

public class RdmaMappedFile {
  private static final Method mmap;
  private static final Method unmmap;
  private static final int ACCESS = IbvMr.IBV_ACCESS_REMOTE_READ;

  private File file = null;
  private FileChannel fileChannel = null;

  private final IbvPd ibvPd;

  private final RdmaMapTaskOutput rdmaMapTaskOutput;
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
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  public RdmaMappedFile(File file, IbvPd ibvPd, int chunkSize, long[] partitionLengths)
      throws IOException, InvocationTargetException, IllegalAccessException {
    this.file = file;
    this.ibvPd = ibvPd;

    final RandomAccessFile backingFile = new RandomAccessFile(file, "rw");
    this.fileChannel = backingFile.getChannel();

    rdmaMapTaskOutput = new RdmaMapTaskOutput(0, partitionLengths.length - 1);
    mapAndRegister(chunkSize, partitionLengths);

    fileChannel.close();
    fileChannel = null;

    file.deleteOnExit();
  }

  private static long roundUpTo4096(long i) {
    return (i + 0xfffL) & ~0xfffL;
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

  private void mapAndRegister(long fileOffset, long length) throws IOException,
      InvocationTargetException, IllegalAccessException {
    long distanceFromPageBoundary = fileOffset % 4096;
    long alignedOffset = fileOffset - distanceFromPageBoundary;
    long alignedLength = roundUpTo4096(length + distanceFromPageBoundary);
    long mapAddress = (long)mmap.invoke(fileChannel, 1, alignedOffset, alignedLength);
    long address = mapAddress + distanceFromPageBoundary;

    if (length > Integer.MAX_VALUE) {
      throw new IOException("Registering files larger than " + Integer.MAX_VALUE + "B is not " +
        "supported");
    }

    SVCRegMr svcRegMr = ibvPd.regMr(address, (int)length, ACCESS).execute();
    IbvMr ibvMr = svcRegMr.getMr();
    svcRegMr.free();

    rdmaFileMappings.add(new RdmaFileMapping(ibvMr, address, mapAddress, length, alignedLength));
  }

  private void unregisterAndUnmap(RdmaFileMapping rdmaFileMapping) throws InvocationTargetException,
      IllegalAccessException, IOException {
    rdmaFileMapping.ibvMr.deregMr().execute().free();
    unmmap.invoke(null, rdmaFileMapping.mapAddress, rdmaFileMapping.alignedLength);
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
    Class<?> classDirectByteBuffer;
    try {
      classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
    } catch (ClassNotFoundException e) {
      throw new IOException("java.nio.DirectByteBuffer class not found");
    }
    Constructor<?> constructor;
    try {
      constructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
    } catch (NoSuchMethodException e) {
      throw new IOException("java.nio.DirectByteBuffer constructor not found");
    }
    constructor.setAccessible(true);
    ByteBuffer byteBuffer;
    try {
      byteBuffer = (ByteBuffer)constructor.newInstance(address, length);
    } catch (Exception e) {
      throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
    }

    return byteBuffer;
  }

  public ByteBuffer getByteBufferForPartition(int partitionId) throws IOException {
    RdmaBlockLocation rdmaBlockLocation = rdmaMapTaskOutput.getRdmaBlockLocation(partitionId);
    return (rdmaBlockLocation.length() == 0) ? null :
      getByteBuffer(rdmaBlockLocation.address(), rdmaBlockLocation.length());
  }
}
