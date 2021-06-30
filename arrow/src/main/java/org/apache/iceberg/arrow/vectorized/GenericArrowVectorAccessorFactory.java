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

package org.apache.iceberg.arrow.vectorized;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/**
 * This class is creates typed {@link ArrowVectorAccessor} from {@link VectorHolder}. It provides a generic
 * implementation for following Arrow types:
 * <ul>
 *   <li>Decimal type can be deserialized to a type that supports decimal,
 *   e.g. BigDecimal or Spark's Decimal.</li>
 *   <li>UTF8 String type can deserialized to a Java String or Spark's UTF8String.</li>
 *   <li>List type: the child elements of a list can be deserialized to Spark's ColumnarArray or similar type.</li>
 *   <li>Struct type: the child elements of a struct can be deserialized to a Spark's ArrowColumnVector
 *   or similar type.</li>
 * </ul>
 *
 * @param <DecimalT>     A concrete type that can represent a decimal.
 * @param <Utf8StringT>  A concrete type that can represent a UTF8 string.
 * @param <ArrayT>       A concrete type that can represent an array value in a list vector, e.g. Spark's
 *                       ColumnarArray.
 * @param <ChildVectorT> A concrete type that can represent a child vector in a struct, e.g. Spark's ArrowColumnVector.
 */
public class GenericArrowVectorAccessorFactory<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable> {

  private final Supplier<DecimalFactory<DecimalT>> decimalFactorySupplier;
  private final Supplier<StringFactory<Utf8StringT>> stringFactorySupplier;
  private final Supplier<StructChildFactory<ChildVectorT>> structChildFactorySupplier;
  private final Supplier<ArrayFactory<ChildVectorT, ArrayT>> arrayFactorySupplier;

  /**
   * The constructor is parameterized using the decimal, string, struct and array factories. If a specific type is not
   * supported, the factory supplier can raise an {@link UnsupportedOperationException}.
   */
  protected GenericArrowVectorAccessorFactory(
      Supplier<DecimalFactory<DecimalT>> decimalFactorySupplier,
      Supplier<StringFactory<Utf8StringT>> stringFactorySupplier,
      Supplier<StructChildFactory<ChildVectorT>> structChildFactorySupplier,
      Supplier<ArrayFactory<ChildVectorT, ArrayT>> arrayFactorySupplier) {
    this.decimalFactorySupplier = decimalFactorySupplier;
    this.stringFactorySupplier = stringFactorySupplier;
    this.structChildFactorySupplier = structChildFactorySupplier;
    this.arrayFactorySupplier = arrayFactorySupplier;
  }

  public ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> getVectorAccessor(VectorHolder holder) {
    Dictionary dictionary = holder.dictionary();
    boolean isVectorDictEncoded = holder.isDictionaryEncoded();
    FieldVector vector = holder.vector();
    if (isVectorDictEncoded) {
      ColumnDescriptor desc = holder.descriptor();
      PrimitiveType primitive = desc.getPrimitiveType();
      return getDictionaryVectorAccessor(dictionary, desc, vector, primitive);
    } else {
      return getPlainVectorAccessor(vector);
    }
  }

  private ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> getDictionaryVectorAccessor(
      Dictionary dictionary,
      ColumnDescriptor desc,
      FieldVector vector, PrimitiveType primitive) {
    Preconditions.checkState(vector instanceof IntVector, "Dictionary ids should be stored in IntVectors only");
    if (primitive.getOriginalType() != null) {
      switch (desc.getPrimitiveType().getOriginalType()) {
        case ENUM:
        case JSON:
        case UTF8:
        case BSON:
          return new DictionaryStringAccessor<>((IntVector) vector, dictionary, stringFactorySupplier.get());
        case INT_64:
        case TIME_MICROS:
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return new DictionaryLongAccessor<>((IntVector) vector, dictionary);
        case DECIMAL:
          switch (primitive.getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
              return new DictionaryDecimalBinaryAccessor<>(
                  (IntVector) vector,
                  dictionary,
                  decimalFactorySupplier.get());
            case INT64:
              return new DictionaryDecimalLongAccessor<>(
                  (IntVector) vector,
                  dictionary,
                  decimalFactorySupplier.get());
            case INT32:
              return new DictionaryDecimalIntAccessor<>(
                  (IntVector) vector,
                  dictionary,
                  decimalFactorySupplier.get());
            default:
              throw new UnsupportedOperationException(
                  "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
          }
        default:
          throw new UnsupportedOperationException(
              "Unsupported logical type: " + primitive.getOriginalType());
      }
    } else {
      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return new DictionaryBinaryAccessor<>((IntVector) vector, dictionary);
        case FLOAT:
          return new DictionaryFloatAccessor<>((IntVector) vector, dictionary);
        case INT64:
          return new DictionaryLongAccessor<>((IntVector) vector, dictionary);
        case DOUBLE:
          return new DictionaryDoubleAccessor<>((IntVector) vector, dictionary);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> getPlainVectorAccessor(FieldVector vector) {
    if (vector instanceof BitVector) {
      return new BooleanAccessor<>((BitVector) vector);
    } else if (vector instanceof IntVector) {
      return new IntAccessor<>((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return new LongAccessor<>((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      return new FloatAccessor<>((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return new DoubleAccessor<>((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      return new DecimalAccessor<>((DecimalVector) vector, decimalFactorySupplier.get());
    } else if (vector instanceof VarCharVector) {
      return new StringAccessor<>((VarCharVector) vector, stringFactorySupplier.get());
    } else if (vector instanceof VarBinaryVector) {
      return new BinaryAccessor<>((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new DateAccessor<>((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      return new TimestampMicroTzAccessor<>((TimeStampMicroTZVector) vector);
    } else if (vector instanceof TimeStampMicroVector) {
      return new TimestampMicroAccessor<>((TimeStampMicroVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      return new ArrayAccessor<>(listVector, arrayFactorySupplier.get());
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      return new StructAccessor<>(structVector, structChildFactorySupplier.get());
    } else if (vector instanceof TimeMicroVector) {
      return new TimeMicroAccessor<>((TimeMicroVector) vector);
    } else if (vector instanceof FixedSizeBinaryVector) {
      return new FixedSizeBinaryAccessor<>((FixedSizeBinaryVector) vector);
    }
    throw new UnsupportedOperationException("Unsupported vector: " + vector.getClass());
  }

  private static class BooleanAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final BitVector vector;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean getBoolean(int rowId) {
      return vector.get(rowId) == 1;
    }
  }

  private static class IntAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final IntVector vector;

    IntAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final int getInt(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final long getLong(int rowId) {
      return getInt(rowId);
    }
  }

  private static class LongAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final BigIntVector vector;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DictionaryLongAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final long[] decodedDictionary;

    DictionaryLongAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToLong(dictionary::decodeToLong)
          .toArray();
    }

    @Override
    public final long getLong(int rowId) {
      return decodedDictionary[offsetVector.get(rowId)];
    }
  }

  private static class FloatAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final Float4Vector vector;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final float getFloat(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final double getDouble(int rowId) {
      return getFloat(rowId);
    }
  }

  private static class DictionaryFloatAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final float[] decodedDictionary;

    DictionaryFloatAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = new float[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        decodedDictionary[i] = dictionary.decodeToFloat(i);
      }
    }

    @Override
    public final float getFloat(int rowId) {
      return decodedDictionary[offsetVector.get(rowId)];
    }

    @Override
    public final double getDouble(int rowId) {
      return getFloat(rowId);
    }
  }

  private static class DoubleAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final Float8Vector vector;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final double getDouble(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DictionaryDoubleAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final double[] decodedDictionary;

    DictionaryDoubleAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToDouble(dictionary::decodeToDouble)
          .toArray();
    }

    @Override
    public final double getDouble(int rowId) {
      return decodedDictionary[offsetVector.get(rowId)];
    }
  }

  private static class StringAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final VarCharVector vector;
    private final StringFactory<Utf8StringT> stringFactory;

    StringAccessor(VarCharVector vector, StringFactory<Utf8StringT> stringFactory) {
      super(vector);
      this.vector = vector;
      this.stringFactory = stringFactory;
    }

    @Override
    public final Utf8StringT getUTF8String(int rowId) {
      return stringFactory.ofRow(vector, rowId);
    }
  }

  private static class DictionaryStringAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final Utf8StringT[] decodedDictionary;
    private final IntVector offsetVector;

    DictionaryStringAccessor(IntVector vector, Dictionary dictionary, StringFactory<Utf8StringT> stringFactory) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToObj(dictionary::decodeToBinary)
          .map(binary -> stringFactory.ofBytes(binary.getBytes()))
          .toArray(genericArray(stringFactory.getGenericClass()));
    }

    @Override
    public final Utf8StringT getUTF8String(int rowId) {
      int offset = offsetVector.get(rowId);
      return decodedDictionary[offset];
    }
  }

  private static class BinaryAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final VarBinaryVector vector;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final byte[] getBinary(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DictionaryBinaryAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final byte[][] decodedDictionary;

    DictionaryBinaryAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToObj(dictionary::decodeToBinary)
          .map(Binary::getBytes)
          .toArray(byte[][]::new);
    }

    @Override
    public final byte[] getBinary(int rowId) {
      int offset = offsetVector.get(rowId);
      return decodedDictionary[offset];
    }
  }

  private static class DateAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final DateDayVector vector;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final int getInt(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class TimestampMicroTzAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final TimeStampMicroTZVector vector;

    TimestampMicroTzAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class TimestampMicroAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final TimeStampMicroVector vector;

    TimestampMicroAccessor(TimeStampMicroVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class TimeMicroAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final TimeMicroVector vector;

    TimeMicroAccessor(TimeMicroVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class FixedSizeBinaryAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final FixedSizeBinaryVector vector;

    FixedSizeBinaryAccessor(FixedSizeBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public byte[] getBinary(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class ArrayAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final ListVector vector;
    private final ChildVectorT arrayData;
    private final ArrayFactory<ChildVectorT, ArrayT> arrayFactory;

    ArrayAccessor(ListVector vector, ArrayFactory<ChildVectorT, ArrayT> arrayFactory) {
      super(vector);
      this.vector = vector;
      this.arrayFactory = arrayFactory;
      this.arrayData = arrayFactory.ofChild(vector.getDataVector());
    }

    @Override
    public final ArrayT getArray(int rowId) {
      return arrayFactory.ofRow(vector, arrayData, rowId);
    }
  }

  private static class StructAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    StructAccessor(StructVector structVector, StructChildFactory<ChildVectorT> structChildFactory) {
      super(structVector, IntStream.range(0, structVector.size())
          .mapToObj(structVector::getVectorById)
          .map(structChildFactory::of)
          .toArray(genericArray(structChildFactory.getGenericClass())));
    }
  }

  private static class DecimalAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final DecimalVector vector;
    private final DecimalFactory<DecimalT> decimalFactory;

    DecimalAccessor(DecimalVector vector, DecimalFactory<DecimalT> decimalFactory) {
      super(vector);
      this.vector = vector;
      this.decimalFactory = decimalFactory;
    }

    @Override
    public final DecimalT getDecimal(int rowId, int precision, int scale) {
      return decimalFactory.ofBigDecimal(
          DecimalUtility.getBigDecimalFromArrowBuf(vector.getDataBuffer(), rowId, scale),
          precision, scale);
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  private abstract static class DictionaryDecimalAccessor<DecimalT, Utf8StringT, ArrayT,
      ChildVectorT extends AutoCloseable> extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final DecimalT[] cache;
    private final DecimalFactory<DecimalT> decimalFactory;
    private final Dictionary parquetDictionary;
    private final IntVector offsetVector;

    private DictionaryDecimalAccessor(
        IntVector vector,
        Dictionary dictionary,
        DecimalFactory<DecimalT> decimalFactory) {
      super(vector);
      this.offsetVector = vector;
      this.parquetDictionary = dictionary;
      this.decimalFactory = decimalFactory;
      this.cache = genericArray(decimalFactory.getGenericClass(), dictionary.getMaxId() + 1);
    }

    protected long decodeToBinary(int dictId) {
      return new BigInteger(parquetDictionary.decodeToBinary(dictId).getBytes()).longValue();
    }

    protected long decodeToLong(int dictId) {
      return parquetDictionary.decodeToLong(dictId);
    }

    protected int decodeToInt(int dictId) {
      return parquetDictionary.decodeToInt(dictId);
    }

    @Override
    public final DecimalT getDecimal(int rowId, int precision, int scale) {
      int dictId = offsetVector.get(rowId);
      if (cache[dictId] == null) {
        cache[dictId] = decimalFactory.ofLong(
            decode(dictId),
            precision,
            scale);
      }
      return cache[dictId];
    }

    protected abstract long decode(int dictId);
  }

  private static class DictionaryDecimalBinaryAccessor<DecimalT, Utf8StringT, ArrayT,
      ChildVectorT extends AutoCloseable>
      extends DictionaryDecimalAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    DictionaryDecimalBinaryAccessor(IntVector vector, Dictionary dictionary, DecimalFactory<DecimalT> decimalFactory) {
      super(vector, dictionary, decimalFactory);
    }

    @Override
    protected long decode(int dictId) {
      return decodeToBinary(dictId);
    }
  }

  private static class DictionaryDecimalLongAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends DictionaryDecimalAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    DictionaryDecimalLongAccessor(IntVector vector, Dictionary dictionary, DecimalFactory<DecimalT> decimalFactory) {
      super(vector, dictionary, decimalFactory);
    }

    @Override
    protected long decode(int dictId) {
      return decodeToLong(dictId);
    }
  }

  private static class DictionaryDecimalIntAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends DictionaryDecimalAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    DictionaryDecimalIntAccessor(IntVector vector, Dictionary dictionary, DecimalFactory<DecimalT> decimalFactory) {
      super(vector, dictionary, decimalFactory);
    }

    @Override
    protected long decode(int dictId) {
      return decodeToInt(dictId);
    }
  }

  /**
   * Create a decimal value of type {@code DecimalT} from arrow vector value.
   *
   * @param <DecimalT> A concrete type that can represent a decimal, e.g, Spark's Decimal.
   */
  protected interface DecimalFactory<DecimalT> {
    /**
     * Class of concrete decimal type.
     */
    Class<DecimalT> getGenericClass();

    /**
     * Create a decimal from the given long value, precision and scale.
     */
    DecimalT ofLong(long value, int precision, int scale);

    /**
     * Create a decimal from the given {@link BigDecimal} value, precision and scale.
     */
    DecimalT ofBigDecimal(BigDecimal value, int precision, int scale);
  }

  /**
   * Create a UTF8 String value of type {@code Utf8StringT} from arrow vector value.
   *
   * @param <Utf8StringT> A concrete type that can represent a UTF8 string.
   */
  protected interface StringFactory<Utf8StringT> {
    /**
     * Class of concrete UTF8 String type.
     */
    Class<Utf8StringT> getGenericClass();

    /**
     * Create a UTF8 String from the row value in the arrow vector.
     */
    Utf8StringT ofRow(VarCharVector vector, int rowId);

    /**
     * Create a UTF8 String from the byte array.
     */
    Utf8StringT ofBytes(byte[] bytes);
  }

  /**
   * Create an array value of type {@code ArrayT} from arrow vector value.
   *
   * @param <ArrayT>       A concrete type that can represent an array value in a list vector, e.g. Spark's
   *                       ColumnarArray.
   * @param <ChildVectorT> A concrete type that can represent a child vector in a struct, e.g. Spark's
   *                       ArrowColumnVector.
   */
  protected interface ArrayFactory<ChildVectorT, ArrayT> {
    /**
     * Create a child vector of type {@code ChildVectorT} from the arrow child vector.
     */
    ChildVectorT ofChild(ValueVector childVector);

    /**
     * Create an Arrow of type {@code ArrayT} from the row value in the arrow child vector.
     */
    ArrayT ofRow(ValueVector vector, ChildVectorT childData, int rowId);
  }

  /**
   * Create a struct child vector of type {@code ChildVectorT} from arrow vector value.
   *
   * @param <ChildVectorT> A concrete type that can represent a child vector in a struct, e.g. Spark's
   *                       ArrowColumnVector.
   */
  protected interface StructChildFactory<ChildVectorT> {
    /**
     * Class of concrete child vector type.
     */
    Class<ChildVectorT> getGenericClass();

    /**
     * Create the child vector of type such as Spark's ArrowColumnVector from the arrow child vector.
     */
    ChildVectorT of(ValueVector childVector);
  }

  private static <T> IntFunction<T[]> genericArray(Class<T> genericClass) {
    return length -> genericArray(genericClass, length);
  }

  @SuppressWarnings("unchecked")
  private static <T> T[] genericArray(Class<T> genericClass, int length) {
    return (T[]) Array.newInstance(genericClass, length);
  }
}
