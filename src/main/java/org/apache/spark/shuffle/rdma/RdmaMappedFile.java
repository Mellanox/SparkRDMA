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
import scala.Int;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import sun.nio.ch.FileChannelImpl;

public class RdmaMappedFile {
  private static final Unsafe unsafe;
  private static final Method mmap;
  private static final Method unmmap;
  private static final int BYTE_ARRAY_OFFSET;
  private static final int ACCESS = IbvMr.IBV_ACCESS_REMOTE_READ;

  private File file = null;
  private FileChannel fileChannel = null;

  private final IbvPd ibvPd;
  private final long fileLength;

  private class RdmaPartitionBlockLocations {
    private final ByteBuffer byteBuffer;
    private static final int ENTRY_SIZE = 8 + 4 + 4;

    RdmaPartitionBlockLocations(int numPartitions) {
      byteBuffer = ByteBuffer.allocate(numPartitions * ENTRY_SIZE);
    }

    RdmaBlockLocation get(int partitionId) {
      return new RdmaBlockLocation(
        byteBuffer.getLong(partitionId * ENTRY_SIZE),
        byteBuffer.getInt(partitionId * ENTRY_SIZE + 8),
        byteBuffer.getInt(partitionId * ENTRY_SIZE + 8 + 4));
    }

    void put(long address, int length, int mKey) {
      byteBuffer.putLong(address);
      byteBuffer.putInt(length);
      byteBuffer.putInt(mKey);
    }
  }
  private RdmaPartitionBlockLocations rdmaPartitionBlockLocations = null;

  private class RdmaFileMapping {
    final IbvMr ibvMr;
    final long fileOffset;
    final long address;
    final long mapAddress;
    final long length;
    final long alignedLength;

    RdmaFileMapping(IbvMr ibvMr, long fileOffset, long address, long mapAddress, long length,
        long alignedLength) {
      this.ibvMr = ibvMr;
      this.fileOffset = fileOffset;
      this.address = address;
      this.mapAddress = mapAddress;
      this.length = length;
      this.alignedLength = alignedLength;
    }
  }
  private final List<RdmaFileMapping> rdmaFileMappings = new ArrayList<>(1);

  static {
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe)unsafeField.get(null);

      mmap = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class,
        long.class);
      mmap.setAccessible(true);
      unmmap = FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
      unmmap.setAccessible(true);
    } catch (Exception e){
      throw new RuntimeException(e);
    }

    BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
  }

  RdmaMappedFile(File file, IbvPd ibvPd) throws IOException, InvocationTargetException,
      IllegalAccessException {
    this(file, ibvPd, file.length());
  }

  public RdmaMappedFile(File file, IbvPd ibvPd, long fileLength) throws IOException,
      InvocationTargetException, IllegalAccessException {
    this.file = file;
    this.fileLength = fileLength;
    this.ibvPd = ibvPd;

    final RandomAccessFile backingFile = new RandomAccessFile(file, "rw");
    backingFile.setLength(roundUpTo4096(this.fileLength));

    this.fileChannel = backingFile.getChannel();

    mapAndRegister();

    fileChannel.close();
    fileChannel = null;

    file.deleteOnExit();
  }

  public RdmaMappedFile(File file, IbvPd ibvPd, int chunkSize, long[] partitionLengths)
      throws IOException, InvocationTargetException, IllegalAccessException {
    this.file = file;
    this.fileLength = file.length();
    this.ibvPd = ibvPd;

    final RandomAccessFile backingFile = new RandomAccessFile(file, "rw");
    this.fileChannel = backingFile.getChannel();

    mapAndRegister(chunkSize, partitionLengths);

    fileChannel.close();
    fileChannel = null;

    file.deleteOnExit();
  }

  private static long roundUpTo4096(long i) {
    return (i + 0xfffL) & ~0xfffL;
  }

  public void write(DirectBuffer buf, long srcOffset, long dstOffset, long length) {
    unsafe.copyMemory(buf.address() + srcOffset, getAddressForOffset(dstOffset, length), length);
  }

  public void write(byte[] bytes, long srcOffset, long dstOffset, long length) {
    unsafe.copyMemory(bytes, BYTE_ARRAY_OFFSET + srcOffset, null,
      getAddressForOffset(dstOffset, length), length);
  }

  private void mapAndRegister(int chunkSize, long[] partitionLengths) throws IOException,
      InvocationTargetException, IllegalAccessException {
    rdmaPartitionBlockLocations = new RdmaPartitionBlockLocations(partitionLengths.length);
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
          rdmaPartitionBlockLocations.put(
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

    if (length > Int.MaxValue()) {
      throw new IOException("Registering files larger than " + Int.MaxValue() +
        "B is not supported");
    }

    SVCRegMr svcRegMr = ibvPd.regMr(address, (int)length, ACCESS).execute();
    IbvMr ibvMr = svcRegMr.getMr();
    svcRegMr.free();

    rdmaFileMappings.add(
      new RdmaFileMapping(ibvMr, fileOffset, address, mapAddress, length, alignedLength));
  }

  private void mapAndRegister() throws IOException, InvocationTargetException,
      IllegalAccessException {
    mapAndRegister(0, fileLength);
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

  public ByteBuffer getByteBuffer() throws IOException {
    return getByteBuffer(getAddressForOffset(0, fileLength), (int)fileLength);
  }

  private RdmaFileMapping getRdmaFileMappingForOffset(long fileOffset, long length) {
    // TODO: improve with binary search
    for (RdmaFileMapping rdmaFileMapping : rdmaFileMappings) {
      if (rdmaFileMapping.fileOffset <= fileOffset &&
        (rdmaFileMapping.fileOffset + rdmaFileMapping.length > fileOffset)) {
        if (fileOffset - rdmaFileMapping.fileOffset + length > rdmaFileMapping.length) {
          throw new IndexOutOfBoundsException(
            "Requested mapping for an offset that is not mapped in full");
        }
        return rdmaFileMapping;
      }
    }

    throw new IndexOutOfBoundsException("Requested mapping for an offset that wasn't mapped");
  }

  private long getAddressForOffset(long fileOffset, long length) {
    RdmaFileMapping rdmaFileMapping = getRdmaFileMappingForOffset(fileOffset, length);
    return rdmaFileMapping.address + fileOffset - rdmaFileMapping.fileOffset;
  }

  public RdmaBlockLocation getRdmaBlockLocationForOffset(long fileOffset, long length) {
    RdmaFileMapping rdmaFileMapping = getRdmaFileMappingForOffset(fileOffset, length);
    return new RdmaBlockLocation(
      rdmaFileMapping.address + fileOffset - rdmaFileMapping.fileOffset,
      (int)length,
      rdmaFileMapping.ibvMr.getLkey());
  }

  public RdmaBlockLocation getRdmaBlockLocationForPartition(int partitionId) {
    return rdmaPartitionBlockLocations.get(partitionId);
  }

  public ByteBuffer getByteBufferForPartition(int partitionId) throws IOException {
    RdmaBlockLocation rdmaBlockLocation = getRdmaBlockLocationForPartition(partitionId);
    return getByteBuffer(rdmaBlockLocation.address(), rdmaBlockLocation.length());
  }
}
