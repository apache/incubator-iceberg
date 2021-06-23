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
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.iceberg.util.NaNUtil;

public class ValueWriters {
  private ValueWriters() {
  }

  public static ValueWriter<Void> nulls() {
    return NullWriter.INSTANCE;
  }

  public static ValueWriter<Boolean> booleans(int id) {
    return new BooleanWriter(id);
  }

  public static ValueWriter<Byte> tinyints(int id) {
    return new ByteToIntegerWriter(id);
  }

  public static ValueWriter<Short> shorts(int id) {
    return new ShortToIntegerWriter(id);
  }

  public static ValueWriter<Integer> ints(int id) {
    return new IntegerWriter(id);
  }

  public static ValueWriter<Long> longs(int id) {
    return new LongWriter(id);
  }

  public static ValueWriter<Float> floats(int id) {
    return new FloatWriter(id);
  }

  public static ValueWriter<Double> doubles(int id) {
    return new DoubleWriter(id);
  }

  public static ValueWriter<CharSequence> strings(int id) {
    return new StringWriter(id);
  }

  public static ValueWriter<Utf8> utf8s(int id) {
    return new Utf8Writer(id);
  }

  public static ValueWriter<UUID> uuids(int id) {
    return new UUIDWriter(id);
  }

  public static ValueWriter<byte[]> fixed(int id, int length) {
    return new FixedWriter(id, length);
  }

  public static ValueWriter<GenericData.Fixed> genericFixed(int id, int length) {
    return new GenericFixedWriter(id, length);
  }

  public static ValueWriter<byte[]> bytes(int id) {
    return new BytesWriter(id);
  }

  public static ValueWriter<ByteBuffer> byteBuffers(int id) {
    return new ByteBufferWriter(id);
  }

  public static ValueWriter<BigDecimal> decimal(int id, int precision, int scale) {
    return new DecimalWriter(id, precision, scale);
  }

  public static <T> ValueWriter<T> option(int nullIndex, ValueWriter<T> writer, Schema.Type type) {
    if (AvroSchemaUtil.supportsMetrics(type)) {
      return new MetricsOptionWriter<>(nullIndex, writer);
    } else {
      return new OptionWriter<>(nullIndex, writer);
    }
  }

  public static <T> ValueWriter<Collection<T>> array(ValueWriter<T> elementWriter) {
    return new CollectionWriter<>(elementWriter);
  }

  public static <K, V> ValueWriter<Map<K, V>> arrayMap(ValueWriter<K> keyWriter,
                                                       ValueWriter<V> valueWriter) {
    return new ArrayMapWriter<>(keyWriter, valueWriter);
  }

  public static <K, V> ValueWriter<Map<K, V>> map(ValueWriter<K> keyWriter,
                                                  ValueWriter<V> valueWriter) {
    return new MapWriter<>(keyWriter, valueWriter);
  }

  public static ValueWriter<IndexedRecord> record(List<ValueWriter<?>> writers) {
    return new RecordWriter(writers);
  }

  /**
   * NullWriter is created as a placeholder so that when building writers from schema,
   * visitor could use the existence of NullWriter for input verification when constructing optional fields.
   * The actual writing of null values is handled by {@link OptionWriter} or {@link MetricsOptionWriter}.
   */
  private static class NullWriter implements ValueWriter<Void> {
    private static final NullWriter INSTANCE = new NullWriter();

    private NullWriter() {
    }

    @Override
    public void write(Void ignored, Encoder encoder) throws IOException {
      encoder.writeNull();
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.empty();
    }
  }

  private static class BooleanWriter extends ComparableWriter<Boolean> {
    private BooleanWriter(int id) {
      super(id);
    }

    @Override
    protected void writeVal(Boolean bool, Encoder encoder) throws IOException {
      encoder.writeBoolean(bool);
    }
  }

  private static class ByteToIntegerWriter extends StoredAsIntWriter<Byte> {
    private ByteToIntegerWriter(int id) {
      super(id);
    }

    @Override
    protected int convert(Byte from) {
      return from.intValue();
    }
  }

  private static class ShortToIntegerWriter extends StoredAsIntWriter<Short> {
    private ShortToIntegerWriter(int id) {
      super(id);
    }

    @Override
    protected int convert(Short from) {
      return from.intValue();
    }
  }

  private static class IntegerWriter extends ComparableWriter<Integer> {
    private IntegerWriter(int id) {
      super(id);
    }

    @Override
    protected void writeVal(Integer i, Encoder encoder) throws IOException {
      encoder.writeInt(i);
    }
  }

  private static class LongWriter extends ComparableWriter<Long> {
    private LongWriter(int id) {
      super(id);
    }

    @Override
    protected void writeVal(Long l, Encoder encoder) throws IOException {
      encoder.writeLong(l);
    }
  }

  private static class FloatWriter extends FloatingPointWriter<Float> {
    private FloatWriter(int id) {
      super(id);
    }

    @Override
    protected void writeVal(Float f, Encoder encoder) throws IOException {
      encoder.writeFloat(f);
    }
  }

  private static class DoubleWriter extends FloatingPointWriter<Double> {
    private DoubleWriter(int id) {
      super(id);
    }

    @Override
    protected void writeVal(Double d, Encoder encoder) throws IOException {
      encoder.writeDouble(d);
    }
  }

  private static class StringWriter extends MetricsAwareWriter<CharSequence> {
    private StringWriter(int id) {
      super(id, Comparators.charSequences());
    }

    @Override
    public void write(CharSequence s, Encoder encoder) throws IOException {
      // use getBytes because it may return the backing byte array if available.
      // otherwise, it copies to a new byte array, which is still cheaper than Avro
      // calling toString, which incurs encoding costs
      if (s instanceof Utf8) {
        super.write(s, encoder);
      } else if (s instanceof String) {
        super.write(new Utf8((String) s), encoder);
      } else if (s == null) {
        throw new IllegalArgumentException("Cannot write null to required string column");
      } else {
        throw new IllegalArgumentException(
            "Cannot write unknown string type: " + s.getClass().getName() + ": " + s.toString());
      }
    }

    @Override
    protected void writeVal(CharSequence s, Encoder encoder) throws IOException {
      encoder.writeString((Utf8) s);
    }
  }

  private static class Utf8Writer extends ComparableWriter<Utf8> {
    private Utf8Writer(int id) {
      super(id);
    }

    @Override
    protected void writeVal(Utf8 s, Encoder encoder) throws IOException {
      encoder.writeString(s);
    }
  }

  private static class UUIDWriter extends MetricsAwareWriter<UUID> {
    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.order(ByteOrder.BIG_ENDIAN);
      return buffer;
    });

    private UUIDWriter(int id) {
      super(id, Comparators.forType(Types.UUIDType.get()));
    }

    @Override
    @SuppressWarnings("ByteBufferBackingArray")
    protected void writeVal(UUID uuid, Encoder encoder) throws IOException {
      // TODO: direct conversion from string to byte buffer
      ByteBuffer buffer = BUFFER.get();
      buffer.rewind();
      buffer.putLong(uuid.getMostSignificantBits());
      buffer.putLong(uuid.getLeastSignificantBits());
      encoder.writeFixed(buffer.array());
    }
  }

  private static class FixedWriter extends MetricsAwareByteArrayWriter {
    private final int length;

    private FixedWriter(int id, int length) {
      super(id);
      this.length = length;
    }

    @Override
    protected void writeVal(byte[] bytes, Encoder encoder) throws IOException {
      Preconditions.checkArgument(bytes.length == length,
          "Cannot write byte array of length %s as fixed[%s]", bytes.length, length);
      encoder.writeFixed(bytes);
    }
  }

  private static class GenericFixedWriter extends ComparableWriter<GenericData.Fixed> {
    private final int length;

    private GenericFixedWriter(int id, int length) {
      super(id);
      this.length = length;
    }

    @Override
    protected void writeVal(GenericData.Fixed datum, Encoder encoder) throws IOException {
      Preconditions.checkArgument(datum.bytes().length == length,
          "Cannot write byte array of length %s as fixed[%s]", datum.bytes().length, length);
      encoder.writeFixed(datum.bytes());
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      // convert min/max to byte buffer to allow upper/lower bound truncation when gathering metrics.
      return metrics(fixed -> ByteBuffer.wrap(fixed.bytes()));
    }
  }

  private static class BytesWriter extends MetricsAwareByteArrayWriter {
    private BytesWriter(int id) {
      super(id);
    }

    @Override
    protected void writeVal(byte[] bytes, Encoder encoder) throws IOException {
      encoder.writeBytes(bytes);
    }
  }

  private static class ByteBufferWriter extends MetricsAwareWriter<ByteBuffer> {
    private ByteBufferWriter(int id) {
      super(id, Comparators.unsignedBytes());
    }

    @Override
    protected void writeVal(ByteBuffer bytes, Encoder encoder) throws IOException {
      encoder.writeBytes(bytes);
    }
  }

  private static class DecimalWriter extends ComparableWriter<BigDecimal> {
    private final int precision;
    private final int scale;
    private final ThreadLocal<byte[]> bytes;

    private DecimalWriter(int id, int precision, int scale) {
      super(id);
      this.precision = precision;
      this.scale = scale;
      this.bytes = ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
    }

    @Override
    protected void writeVal(BigDecimal decimal, Encoder encoder) throws IOException {
      encoder.writeFixed(DecimalUtil.toReusedFixLengthBytes(precision, scale, decimal, bytes.get()));
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  private static class OptionWriter<T> implements ValueWriter<T> {
    private final int nullIndex;
    private final int valueIndex;
    protected final ValueWriter<T> valueWriter;

    private OptionWriter(int nullIndex, ValueWriter<T> valueWriter) {
      this.nullIndex = nullIndex;
      if (nullIndex == 0) {
        this.valueIndex = 1;
      } else if (nullIndex == 1) {
        this.valueIndex = 0;
      } else {
        throw new IllegalArgumentException("Invalid option index: " + nullIndex);
      }
      this.valueWriter = valueWriter;
    }

    @Override
    public void write(T option, Encoder encoder) throws IOException {
      if (option == null) {
        encoder.writeIndex(nullIndex);
      } else {
        encoder.writeIndex(valueIndex);
        valueWriter.write(option, encoder);
      }
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return valueWriter.metrics();
    }
  }

  private static class MetricsOptionWriter<T> extends OptionWriter<T> {
    private long nullValueCount;

    private MetricsOptionWriter(int nullIndex, ValueWriter<T> valueWriter) {
      super(nullIndex, valueWriter);
      this.nullValueCount = 0;
    }

    @Override
    public void write(T option, Encoder encoder) throws IOException {
      super.write(option, encoder);
      if (option == null) {
        nullValueCount++;
      }
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      List<FieldMetrics> fieldMetricsFromWriter = valueWriter.metrics().collect(Collectors.toList());
      Preconditions.checkState(fieldMetricsFromWriter.size() == 1,
          "MetricsOptionWriter should not merge null metrics for more than one field. " +
              "Current number of fields: %s", fieldMetricsFromWriter.size());

      FieldMetrics metrics = fieldMetricsFromWriter.get(0);
      return Stream.of(
          new FieldMetrics<>(metrics.id(),
              metrics.valueCount() + nullValueCount, nullValueCount,
              metrics.nanValueCount(), metrics.lowerBound(), metrics.upperBound())
      );
    }
  }

  private static class CollectionWriter<T> implements ValueWriter<Collection<T>> {
    private final ValueWriter<T> elementWriter;

    private CollectionWriter(ValueWriter<T> elementWriter) {
      this.elementWriter = elementWriter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Collection<T> array, Encoder encoder) throws IOException {
      encoder.writeArrayStart();
      int numElements = array.size();
      encoder.setItemCount(numElements);
      Iterator<T> iter = array.iterator();
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        elementWriter.write(iter.next(), encoder);
      }
      encoder.writeArrayEnd();
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return elementWriter.metrics();
    }
  }

  private static class ArrayMapWriter<K, V> implements ValueWriter<Map<K, V>> {
    private final ValueWriter<K> keyWriter;
    private final ValueWriter<V> valueWriter;

    private ArrayMapWriter(ValueWriter<K> keyWriter, ValueWriter<V> valueWriter) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Map<K, V> map, Encoder encoder) throws IOException {
      encoder.writeArrayStart();
      int numElements = map.size();
      encoder.setItemCount(numElements);
      Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        Map.Entry<K, V> entry = iter.next();
        keyWriter.write(entry.getKey(), encoder);
        valueWriter.write(entry.getValue(), encoder);
      }
      encoder.writeArrayEnd();
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.concat(keyWriter.metrics(), valueWriter.metrics());
    }
  }

  private static class MapWriter<K, V> implements ValueWriter<Map<K, V>> {
    private final ValueWriter<K> keyWriter;
    private final ValueWriter<V> valueWriter;

    private MapWriter(ValueWriter<K> keyWriter, ValueWriter<V> valueWriter) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Map<K, V> map, Encoder encoder) throws IOException {
      encoder.writeMapStart();
      int numElements = map.size();
      encoder.setItemCount(numElements);
      Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        Map.Entry<K, V> entry = iter.next();
        keyWriter.write(entry.getKey(), encoder);
        valueWriter.write(entry.getValue(), encoder);
      }
      encoder.writeMapEnd();
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.concat(keyWriter.metrics(), valueWriter.metrics());
    }
  }

  public abstract static class StructWriter<S> implements ValueWriter<S> {
    private final ValueWriter<Object>[] writers;

    @SuppressWarnings("unchecked")
    protected StructWriter(List<ValueWriter<?>> writers) {
      this.writers = (ValueWriter<Object>[]) Array.newInstance(ValueWriter.class, writers.size());
      for (int i = 0; i < this.writers.length; i += 1) {
        this.writers[i] = (ValueWriter<Object>) writers.get(i);
      }
    }

    protected abstract Object get(S struct, int pos);

    public ValueWriter<?> writer(int pos) {
      return writers[pos];
    }

    @Override
    public void write(S row, Encoder encoder) throws IOException {
      for (int i = 0; i < writers.length; i += 1) {
        writers[i].write(get(row, i), encoder);
      }
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Arrays.stream(writers).flatMap(ValueWriter::metrics);
    }
  }

  private static class RecordWriter extends StructWriter<IndexedRecord> {
    @SuppressWarnings("unchecked")
    private RecordWriter(List<ValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(IndexedRecord struct, int pos) {
      return struct.get(pos);
    }
  }

  private abstract static class FloatingPointWriter<T extends Comparable<T>>
      extends MetricsAwareWriter<T> {
    private long nanValueCount;

    FloatingPointWriter(int id) {
      // pass null to comparator since we override write() method that uses comparator in this class.
      super(id, null);
    }

    @Override
    public void write(T datum, Encoder encoder) throws IOException {
      valueCount++;

      // null value should be handled by option writer, thus assume datum will not be null here.
      if (NaNUtil.isNaN(datum)) {
        nanValueCount++;
      } else {
        if (max == null || datum.compareTo(max) > 0) {
          this.max = datum;
        }

        if (min == null || datum.compareTo(min) < 0) {
          this.min = datum;
        }
      }

      writeVal(datum, encoder);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.of(new FieldMetrics<>(id, valueCount, 0, nanValueCount, min, max));
    }
  }

  public abstract static class MetricsAwareStringWriter<T extends Comparable<T>> extends ComparableWriter<T> {
    public MetricsAwareStringWriter(int id) {
      super(id);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      // convert min/max to string to allow upper/lower bound truncation when gathering metrics,
      // as in different implementations there's no guarantee that input to string writer will be char sequence
      return metrics(Object::toString);
    }
  }

  private abstract static class MetricsAwareByteArrayWriter extends MetricsAwareWriter<byte[]> {
    MetricsAwareByteArrayWriter(int id) {
      super(id, Comparators.unsignedByteArrays());
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      // convert min/max to byte buffer to allow upper/lower bound truncation when gathering metrics.
      return metrics(ByteBuffer::wrap);
    }
  }

  public abstract static class ComparableWriter<T extends Comparable<T>> extends MetricsAwareWriter<T> {
    public ComparableWriter(int id) {
      super(id, Comparable::compareTo);
    }
  }

  /**
   * A value writer wrapper that keeps track of column statistics (metrics) during writing.
   *
   * @param <T> Input type
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public abstract static class MetricsAwareWriter<T> implements ValueWriter<T> {
    protected final int id;
    protected long valueCount;
    protected T max;
    protected T min;

    private final Comparator<T> comparator;

    public MetricsAwareWriter(int id, Comparator<T> comparator) {
      this.id = id;
      this.comparator = comparator;
    }

    @Override
    public void write(T datum, Encoder encoder) throws IOException {
      valueCount++;

      // null value should be handled by option writer, thus assume datum will not be null here.
      if (max == null || comparator.compare(datum, max) > 0) {
        max = datum;
      }

      if (min == null || comparator.compare(datum, min) < 0) {
        min = datum;
      }

      writeVal(datum, encoder);
    }

    protected abstract void writeVal(T datum, Encoder encoder) throws IOException;

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.of(new FieldMetrics<>(id, valueCount, 0, 0, min, max));
    }

    /**
     * Helper class to transform the input type when collecting metrics.
     * The transform function converts the stats information from the specific type that the underlying writer
     * understands to a more general type that could be transformed to binary following iceberg single-value
     * serialization spec.
     *
     * @param func transformation function
     * @return a stream of field metrics with bounds converted by the given transformation
     */
    protected Stream<FieldMetrics> metrics(Function<T, ?> func) {
      return Stream.of(new FieldMetrics<>(id, valueCount, 0, 0,
          min == null ? null : func.apply(min), max == null ? null : func.apply(max)));
    }
  }

  /**
   * A value writer wrapper that keeps track of column statistics (metrics) during writing.
   * Implementations have to supply a convert() function, which will be applied to the input
   * data to produce the integer type that the underlying writer accepts. Stats will also be
   * tracked with int type.
   *
   * @param <T> Input type before conversion
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public abstract static class StoredAsIntWriter<T> implements ValueWriter<T> {
    protected final int id;
    protected long valueCount;
    protected Integer max;
    protected Integer min;

    public StoredAsIntWriter(int id) {
      this.id = id;
    }

    protected abstract int convert(T from);

    @Override
    public void write(T datum, Encoder encoder) throws IOException {
      valueCount++;

      // null value should be handled by option writer, thus assume datum will not be null here.
      int value = convert(datum);

      if (max == null || value > max) {
        max = value;
      }
      if (min == null || value < min) {
        min = value;
      }
      encoder.writeInt(value);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.of(new FieldMetrics<>(id, valueCount, 0, 0, min, max));
    }
  }

  /**
   * A value writer wrapper that keeps track of column statistics (metrics) during writing.
   * Implementations have to supply a convert() function, which will be applied to the input
   * data to produce the long type that the underlying writer accepts. Stats will also be
   * tracked with long type.
   *
   * @param <T> Input type before conversion
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public abstract static class StoredAsLongWriter<T> implements ValueWriter<T> {
    protected final int id;
    protected long valueCount;
    protected Long max;
    protected Long min;

    public StoredAsLongWriter(int id) {
      this.id = id;
    }

    protected abstract long convert(T from);

    @Override
    public void write(T datum, Encoder encoder) throws IOException {
      valueCount++;

      // null value should be handled by option writer, thus assume datum will not be null here.
      long value = convert(datum);

      if (max == null || value > max) {
        max = value;
      }

      if (min == null || value < min) {
        min = value;
      }

      encoder.writeLong(value);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      return Stream.of(new FieldMetrics<>(id, valueCount, 0, 0, min, max));
    }
  }
}
