/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.SerializationUtil;

// Since this class extends `mapreduce.InputSplit and implements `mapred.InputSplit`, it can be returned by both MR v1
// and v2 file formats.
public class IcebergSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit, IcebergSplitContainer {

  public static final String[] ANYWHERE = new String[]{"*"};

  private CombinedScanTask task;
  private FileIO io;
  private EncryptionManager encryptionManager;

  private transient String[] locations;
  private transient Configuration conf;

  // public no-argument constructor for deserialization
  public IcebergSplit() {
  }

  IcebergSplit(Configuration conf, CombinedScanTask task, FileIO io, EncryptionManager encryptionManager) {
    this.task = task;
    this.conf = conf;
    this.io = io;
    this.encryptionManager = encryptionManager;
  }

  public CombinedScanTask task() {
    return task;
  }

  @Override
  public IcebergSplit icebergSplit() {
    return this;
  }

  @Override
  public long getLength() {
    return task.files().stream().mapToLong(FileScanTask::length).sum();
  }

  @Override
  public String[] getLocations() {
    if (locations == null) {
      boolean localityPreferred = conf.getBoolean(InputFormatConfig.LOCALITY, false);
      locations = localityPreferred ? Util.blockLocations(task, conf) : ANYWHERE;
    }

    return locations;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] data = SerializationUtil.serializeToBytes(this.task);
    out.writeInt(data.length);
    out.write(data);

    byte[] ioData;
    if (io instanceof HadoopFileIO) {
      SerializableConfiguration serializableConf = new SerializableConfiguration(((HadoopFileIO) io).conf());
      ioData = SerializationUtil.serializeToBytes(new HadoopFileIO(serializableConf::get));
    } else {
      ioData = SerializationUtil.serializeToBytes(io);
    }
    out.writeInt(ioData.length);
    out.write(ioData);

    byte[] encryptionManagerData = SerializationUtil.serializeToBytes(encryptionManager);
    out.writeInt(encryptionManagerData.length);
    out.write(encryptionManagerData);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] data = new byte[in.readInt()];
    in.readFully(data);
    this.task = SerializationUtil.deserializeFromBytes(data);

    byte[] ioData = new byte[in.readInt()];
    in.readFully(ioData);
    this.io = SerializationUtil.deserializeFromBytes(ioData);

    byte[] encryptionManagerData = new byte[in.readInt()];
    in.readFully(encryptionManagerData);
    this.encryptionManager = SerializationUtil.deserializeFromBytes(encryptionManagerData);
  }

  public FileIO io() {
    return io;
  }

  public EncryptionManager encryptionManager() {
    return encryptionManager;
  }
}
