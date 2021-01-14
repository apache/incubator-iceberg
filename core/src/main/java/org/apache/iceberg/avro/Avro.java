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

package org.apache.iceberg.avro;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ArrayUtil;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_DEFAULT;

public class Avro {
  private Avro() {
  }

  private enum CodecName {
    UNCOMPRESSED(CodecFactory.nullCodec()),
    SNAPPY(CodecFactory.snappyCodec()),
    GZIP(CodecFactory.deflateCodec(9)),
    LZ4(null),
    BROTLI(null),
    ZSTD(null);

    private final CodecFactory avroCodec;

    CodecName(CodecFactory avroCodec) {
      this.avroCodec = avroCodec;
    }

    public CodecFactory get() {
      Preconditions.checkArgument(avroCodec != null, "Missing implementation for codec %s", this);
      return avroCodec;
    }
  }

  private static final GenericData DEFAULT_MODEL = new SpecificData();

  static {
    LogicalTypes.register(LogicalMap.NAME, schema -> LogicalMap.get());
    DEFAULT_MODEL.addLogicalTypeConversion(new Conversions.DecimalConversion());
    DEFAULT_MODEL.addLogicalTypeConversion(new UUIDConversion());
  }

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static class WriteBuilder {
    private final OutputFile file;
    private final Map<String, String> config = Maps.newHashMap();
    private final Map<String, String> metadata = Maps.newLinkedHashMap();
    private org.apache.iceberg.Schema schema = null;
    private String name = "table";
    private Function<Schema, MetricsAwareDatumWriter<?>> createWriterFunc = null;
    private boolean overwrite;
    private MetricsConfig metricsConfig;

    private WriteBuilder(OutputFile file) {
      this.file = file;
    }

    public WriteBuilder forTable(Table table) {
      schema(table.schema());
      setAll(table.properties());
      return this;
    }

    public WriteBuilder schema(org.apache.iceberg.Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public WriteBuilder named(String newName) {
      this.name = newName;
      return this;
    }

    public WriteBuilder createWriterFunc(Function<Schema, MetricsAwareDatumWriter<?>> writerFunction) {
      this.createWriterFunc = writerFunction;
      return this;
    }

    public WriteBuilder set(String property, String value) {
      config.put(property, value);
      return this;
    }

    public WriteBuilder setAll(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    public WriteBuilder meta(String property, String value) {
      metadata.put(property, value);
      return this;
    }

    public WriteBuilder meta(Map<String, String> properties) {
      metadata.putAll(properties);
      return this;
    }

    public WriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return this;
    }

    public WriteBuilder overwrite() {
      return overwrite(true);
    }

    public WriteBuilder overwrite(boolean enabled) {
      this.overwrite = enabled;
      return this;
    }

    private CodecFactory codec() {
      String codec = config.getOrDefault(AVRO_COMPRESSION, AVRO_COMPRESSION_DEFAULT);
      try {
        return CodecName.valueOf(codec.toUpperCase(Locale.ENGLISH)).get();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported compression codec: " + codec);
      }
    }

    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(name, "Table name is required and cannot be null");

      Function<Schema, MetricsAwareDatumWriter<?>> writerFunc;
      if (createWriterFunc != null) {
        writerFunc = createWriterFunc;
      } else {
        writerFunc = GenericAvroWriter::new;
      }

      // add the Iceberg schema to keyValueMetadata
      meta("iceberg.schema", SchemaParser.toJson(schema));

      return new AvroFileAppender<>(
          schema, AvroSchemaUtil.convert(schema, name), file, writerFunc, codec(), metadata, metricsConfig, overwrite);
    }
  }

  public static DeleteWriteBuilder writeDeletes(OutputFile file) {
    return new DeleteWriteBuilder(file);
  }

  public static class DeleteWriteBuilder {
    private final WriteBuilder appenderBuilder;
    private final String location;
    private Function<Schema, MetricsAwareDatumWriter<?>> createWriterFunc = null;
    private org.apache.iceberg.Schema rowSchema;
    private PartitionSpec spec;
    private StructLike partition;
    private EncryptionKeyMetadata keyMetadata = null;
    private int[] equalityFieldIds = null;

    private DeleteWriteBuilder(OutputFile file) {
      this.appenderBuilder = write(file);
      this.location = file.location();
    }

    public DeleteWriteBuilder forTable(Table table) {
      rowSchema(table.schema());
      withSpec(table.spec());
      setAll(table.properties());
      return this;
    }

    public DeleteWriteBuilder set(String property, String value) {
      appenderBuilder.set(property, value);
      return this;
    }

    public DeleteWriteBuilder setAll(Map<String, String> properties) {
      appenderBuilder.setAll(properties);
      return this;
    }

    public DeleteWriteBuilder meta(String property, String value) {
      appenderBuilder.meta(property, value);
      return this;
    }

    public DeleteWriteBuilder meta(Map<String, String> properties) {
      appenderBuilder.meta(properties);
      return this;
    }

    public DeleteWriteBuilder overwrite() {
      return overwrite(true);
    }

    public DeleteWriteBuilder overwrite(boolean enabled) {
      appenderBuilder.overwrite(enabled);
      return this;
    }

    public DeleteWriteBuilder createWriterFunc(Function<Schema, MetricsAwareDatumWriter<?>> writerFunction) {
      this.createWriterFunc = writerFunction;
      return this;
    }

    public DeleteWriteBuilder rowSchema(org.apache.iceberg.Schema newRowSchema) {
      this.rowSchema = newRowSchema;
      return this;
    }

    public DeleteWriteBuilder withSpec(PartitionSpec newSpec) {
      this.spec = newSpec;
      return this;
    }

    public DeleteWriteBuilder withPartition(StructLike key) {
      this.partition = key;
      return this;
    }

    public DeleteWriteBuilder withKeyMetadata(EncryptionKeyMetadata metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public DeleteWriteBuilder equalityFieldIds(List<Integer> fieldIds) {
      this.equalityFieldIds = ArrayUtil.toIntArray(fieldIds);
      return this;
    }

    public DeleteWriteBuilder equalityFieldIds(int... fieldIds) {
      this.equalityFieldIds = fieldIds;
      return this;
    }

    public <T> EqualityDeleteWriter<T> buildEqualityWriter() throws IOException {
      Preconditions.checkState(rowSchema != null, "Cannot create equality delete file without a schema`");
      Preconditions.checkState(equalityFieldIds != null, "Cannot create equality delete file without delete field ids");
      Preconditions.checkState(createWriterFunc != null,
          "Cannot create equality delete file unless createWriterFunc is set");

      meta("delete-type", "equality");
      meta("delete-field-ids", IntStream.of(equalityFieldIds)
          .mapToObj(Objects::toString)
          .collect(Collectors.joining(", ")));

      // the appender uses the row schema without extra columns
      appenderBuilder.schema(rowSchema);
      appenderBuilder.createWriterFunc(createWriterFunc);

      return new EqualityDeleteWriter<>(
          appenderBuilder.build(), FileFormat.AVRO, location, spec, partition, keyMetadata, equalityFieldIds);
    }

    public <T> PositionDeleteWriter<T> buildPositionWriter() throws IOException {
      Preconditions.checkState(equalityFieldIds == null, "Cannot create position delete file using delete field ids");

      meta("delete-type", "position");

      if (rowSchema != null && createWriterFunc != null) {
        // the appender uses the row schema wrapped with position fields
        appenderBuilder.schema(DeleteSchemaUtil.posDeleteSchema(rowSchema));

        appenderBuilder.createWriterFunc(
            avroSchema -> new PositionAndRowDatumWriter<>(createWriterFunc.apply(avroSchema)));

      } else {
        appenderBuilder.schema(DeleteSchemaUtil.pathPosSchema());

        appenderBuilder.createWriterFunc(ignored -> new PositionDatumWriter());
      }

      return new PositionDeleteWriter<>(
          appenderBuilder.build(), FileFormat.AVRO, location, spec, partition, keyMetadata);
    }
  }

  /**
   * A {@link DatumWriter} implementation that wraps another to produce position deletes.
   */
  private static class PositionDatumWriter implements MetricsAwareDatumWriter<PositionDelete<?>> {
    private static final ValueWriter<CharSequence> PATH_WRITER =
        ValueWriters.strings(MetadataColumns.DELETE_FILE_PATH.fieldId());
    private static final ValueWriter<Long> POS_WRITER =
        ValueWriters.longs(MetadataColumns.DELETE_FILE_POS.fieldId());

    @Override
    public void setSchema(Schema schema) {
    }

    @Override
    public void write(PositionDelete<?> delete, Encoder out) throws IOException {
      PATH_WRITER.write(delete.path(), out);
      POS_WRITER.write(delete.pos(), out);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.concat(PATH_WRITER.metrics(), POS_WRITER.metrics());
    }
  }

  /**
   * A {@link DatumWriter} implementation that wraps another to produce position deletes with row data.
   *
   * @param <D> the type of datum written as a deleted row
   */
  private static class PositionAndRowDatumWriter<D> implements MetricsAwareDatumWriter<PositionDelete<D>> {
    private static final ValueWriter<CharSequence> PATH_WRITER =
        ValueWriters.strings(MetadataColumns.DELETE_FILE_PATH.fieldId());
    private static final ValueWriter<Long> POS_WRITER =
        ValueWriters.longs(MetadataColumns.DELETE_FILE_POS.fieldId());

    private final DatumWriter<D> rowWriter;

    private PositionAndRowDatumWriter(DatumWriter<D> rowWriter) {
      this.rowWriter = rowWriter;
    }

    @Override
    public void setSchema(Schema schema) {
      Schema.Field rowField = schema.getField("row");
      if (rowField != null) {
        rowWriter.setSchema(rowField.schema());
      }
    }

    @Override
    public void write(PositionDelete<D> delete, Encoder out) throws IOException {
      PATH_WRITER.write(delete.path(), out);
      POS_WRITER.write(delete.pos(), out);
      rowWriter.write(delete.row(), out);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.concat(PATH_WRITER.metrics(), POS_WRITER.metrics());
    }
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private final Map<String, String> renames = Maps.newLinkedHashMap();
    private ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private NameMapping nameMapping;
    private boolean reuseContainers = false;
    private org.apache.iceberg.Schema schema = null;
    private Function<Schema, DatumReader<?>> createReaderFunc = null;
    private BiFunction<org.apache.iceberg.Schema, Schema, DatumReader<?>> createReaderBiFunc = null;
    private final Function<Schema, DatumReader<?>> defaultCreateReaderFunc = readSchema -> {
      GenericAvroReader<?> reader = new GenericAvroReader<>(readSchema);
      reader.setClassLoader(loader);
      return reader;
    };
    private Long start = null;
    private Long length = null;

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
    }

    public ReadBuilder createReaderFunc(Function<Schema, DatumReader<?>> readerFunction) {
      Preconditions.checkState(createReaderBiFunc == null, "Cannot set multiple createReaderFunc");
      this.createReaderFunc = readerFunction;
      return this;
    }

    public ReadBuilder createReaderFunc(BiFunction<org.apache.iceberg.Schema, Schema, DatumReader<?>> readerFunction) {
      Preconditions.checkState(createReaderFunc == null, "Cannot set multiple createReaderFunc");
      this.createReaderBiFunc = readerFunction;
      return this;
    }

    /**
     * Restricts the read to the given range: [start, end = start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    public ReadBuilder project(org.apache.iceberg.Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    public ReadBuilder reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    public ReadBuilder reuseContainers(boolean shouldReuse) {
      this.reuseContainers = shouldReuse;
      return this;
    }

    public ReadBuilder rename(String fullName, String newName) {
      renames.put(fullName, newName);
      return this;
    }

    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public ReadBuilder classLoader(ClassLoader classLoader) {
      this.loader = classLoader;
      return this;
    }

    public <D> AvroIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      Function<Schema, DatumReader<?>> readerFunc;
      if (createReaderBiFunc != null) {
        readerFunc = avroSchema -> createReaderBiFunc.apply(schema, avroSchema);
      } else if (createReaderFunc != null) {
        readerFunc = createReaderFunc;
      } else {
        readerFunc = defaultCreateReaderFunc;
      }

      return new AvroIterable<>(file,
          new ProjectionDatumReader<>(readerFunc, schema, renames, nameMapping),
          start, length, reuseContainers);
    }
  }

}
