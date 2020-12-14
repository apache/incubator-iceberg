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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.ContinuousIcebergEnumerator;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlanner;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlannerImpl;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorConfig;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorStateSerializer;
import org.apache.iceberg.flink.source.enumerator.StaticIcebergEnumerator;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.reader.ReaderFactory;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;

@Experimental
public class IcebergSource<T> implements Source<T, IcebergSourceSplit, IcebergEnumeratorState> {

  private final TableLoader tableLoader;
  private final ScanContext scanContext;
  private final ReaderFactory<T> readerFactory;
  private final SplitAssignerFactory assignerFactory;
  private final IcebergEnumeratorConfig enumeratorConfig;

  IcebergSource(
      TableLoader tableLoader,
      ScanContext scanContext,
      ReaderFactory<T> readerFactory,
      SplitAssignerFactory assignerFactory,
      IcebergEnumeratorConfig enumeratorConfig) {

    this.tableLoader = tableLoader;
    this.enumeratorConfig = enumeratorConfig;
    this.scanContext = scanContext;
    this.readerFactory = readerFactory;
    this.assignerFactory = assignerFactory;
  }

  private static Table loadTable(TableLoader tableLoader) {
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      return loader.loadTable();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close table loader", e);
    }
  }

  @Override
  public Boundedness getBoundedness() {
    return enumeratorConfig.splitDiscoveryInterval() == null ?
        Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<T, IcebergSourceSplit> createReader(SourceReaderContext readerContext) {
    return new IcebergSourceReader<>(
        readerContext,
        readerFactory);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, IcebergEnumeratorState enumState) {
    return createEnumerator(enumContext, enumState);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return IcebergSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<IcebergEnumeratorState> getEnumeratorCheckpointSerializer() {
    return IcebergEnumeratorStateSerializer.INSTANCE;
  }

  private SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      @Nullable IcebergEnumeratorState enumState) {

    final Table table = loadTable(tableLoader);

    final SplitAssigner assigner;
    if (enumState == null) {
      assigner = assignerFactory.createAssigner();
      // for batch jobs, discover splits eagerly during job initialization.
      // As FLINK-16866 supports non-blocking job submission since 1.12,
      // heavy job initialization won't lead to request timeout for job submission.
      if (enumeratorConfig.splitDiscoveryInterval() == null) {
        assigner.onDiscoveredSplits(FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext));
      }
    } else {
      assigner = assignerFactory.createAssigner(enumState.pendingSplits());
    }

    if (enumeratorConfig.splitDiscoveryInterval() == null) {
      return new StaticIcebergEnumerator(enumContext, assigner);
    } else {
      final ContinuousSplitPlanner splitPlanner = new ContinuousSplitPlannerImpl(
          table, enumeratorConfig, scanContext);
      return new ContinuousIcebergEnumerator(enumContext, assigner,
          enumState, enumeratorConfig, splitPlanner);
    }
  }


  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    // required
    private TableLoader tableLoader;
    private SplitAssignerFactory splitAssignerFactory;
    private ReaderFactory<T> readerFactory;

    // optional
    private ScanContext scanContext;
    private IcebergEnumeratorConfig enumeratorConfig;

    Builder() {
      this.scanContext = ScanContext.builder().build();
      this.enumeratorConfig = IcebergEnumeratorConfig.builder().build();
    }

    public Builder<T> tableLoader(TableLoader loader) {
      this.tableLoader = loader;
      return this;
    }

    public Builder<T> assignerFactory(SplitAssignerFactory assignerFactory) {
      this.splitAssignerFactory = assignerFactory;
      return this;
    }

    public Builder<T> readerFactory(ReaderFactory<T> newReaderFactory) {
      this.readerFactory = newReaderFactory;
      return this;
    }

    public Builder<T> scanContext(ScanContext newScanContext) {
      this.scanContext = newScanContext;
      return this;
    }

    public Builder<T> enumeratorConfig(IcebergEnumeratorConfig newConfig) {
      this.enumeratorConfig = newConfig;
      return this;
    }

    public IcebergSource<T> build() {
      checkRequired();
      return new IcebergSource<>(
          tableLoader,
          scanContext,
          readerFactory,
          splitAssignerFactory,
          enumeratorConfig);
    }

    private void checkRequired() {
      Preconditions.checkNotNull(tableLoader, "tableLoader is required.");
      Preconditions.checkNotNull(splitAssignerFactory, "asignerFactory is required.");
      Preconditions.checkNotNull(readerFactory, "bulkFormat is required.");
    }
  }
}
