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

package org.apache.iceberg.beam;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriter<T extends GenericRecord> extends DoFn<T, DataFile> {
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

  private final PartitionSpec spec;
  private final TableIdentifier tableIdentifier;
  private final String hiveMetastoreUrl;

  private Map<String, String> properties;
  private Schema schema;
  private LocationProvider locations;
  private FileIO io;
  private EncryptionManager encryptionManager;
  private HiveCatalog catalog;
  private FileFormat fileFormat;

  private transient TaskWriter<T> writer;
  private transient BoundedWindow lastSeenWindow;

  public FileWriter(
      TableIdentifier tableIdentifier,
      Schema schema,
      PartitionSpec spec,
      String hiveMetastoreUrl,
      Map<String, String> properties
  ) {
    this.tableIdentifier = tableIdentifier;
    this.spec = spec;
    this.hiveMetastoreUrl = hiveMetastoreUrl;
    this.schema = schema;
    this.properties = properties;
  }

  @StartBundle
  public void startBundle(StartBundleContext sbc) {
    start();
  }

  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    appendRecord(
        context.element(),
        window,
        (int) context.pane().getIndex(),
        context.pane().getIndex()
    );
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext fbc) {
    DataFile[] files = finish();
    for (DataFile file : files) {
      fbc.output(file, Instant.now(), lastSeenWindow);
    }
  }

  @VisibleForTesting
  public void start() {
    Configuration conf = new Configuration();
    for (String key : this.properties.keySet()) {
      conf.set(key, this.properties.get(key));
    }
    catalog = new HiveCatalog(
        HiveCatalog.DEFAULT_NAME,
        this.hiveMetastoreUrl,
        1,
        conf
    );
    Table table = HiveCatalogHelper.loadOrCreateTable(
        catalog,
        tableIdentifier,
        schema,
        spec,
        properties
    );
    this.schema = table.schema();
    this.locations = table.locationProvider();
    this.properties = table.properties();
    this.io = table.io();
    this.encryptionManager = table.encryption();
    String formatString = table.properties().getOrDefault(
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.fileFormat = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  @VisibleForTesting
  public void appendRecord(T element, BoundedWindow window, int partitionId, long taskId) {
    if (writer == null) {
      LOG.info("Setting up the writer");
      // We would rather do this in the startBundle, but we don't know the pane

      BeamAppenderFactory<T> appenderFactory = new BeamAppenderFactory<>(schema, properties, spec);
      OutputFileFactory fileFactory = new OutputFileFactory(
          spec, fileFormat, locations, io, encryptionManager, partitionId, taskId);

      if (spec.isUnpartitioned()) {
        writer = new UnpartitionedWriter<>(spec, fileFormat, appenderFactory, fileFactory, io, Long.MAX_VALUE);
      } else {
        writer = new PartitionedWriter<T>(
            spec, fileFormat, appenderFactory, fileFactory, io, Long.MAX_VALUE) {
          @Override
          protected PartitionKey partition(T row) {
            return new PartitionKey(spec, schema);
          }
        };
      }
    }
    try {
      lastSeenWindow = window;
      writer.write(element);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public DataFile[] finish() {
    LOG.info("Closing the writer");
    try {
      writer.close();
      return writer.dataFiles();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      writer = null;
      catalog.close();
      catalog = null;
    }
  }
}
