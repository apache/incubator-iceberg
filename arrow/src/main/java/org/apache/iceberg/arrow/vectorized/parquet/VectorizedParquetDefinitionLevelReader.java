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

package org.apache.iceberg.arrow.vectorized.parquet;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.column.Dictionary;

public final class VectorizedParquetDefinitionLevelReader extends BaseVectorizedParquetValuesReader {

  public VectorizedParquetDefinitionLevelReader(int bitWidth, int maxDefLevel, boolean setArrowValidityVector) {
    super(bitWidth, maxDefLevel, setArrowValidityVector);
  }

  public VectorizedParquetDefinitionLevelReader(int bitWidth, int maxDefLevel, boolean readLength,
                                                boolean setArrowValidityVector) {
    super(bitWidth, maxDefLevel, readLength, setArrowValidityVector);
  }

  abstract class NumericBaseReader {
    public void nextBatch(
        final FieldVector vector, final int startOffset, final int typeWidth,
        final int numValsToRead, NullabilityHolder nullabilityHolder, ValuesAsBytesReader valuesReader) {
      int bufferIdx = startOffset;
      int left = numValsToRead;
      while (left > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int numValues = Math.min(left, currentCount);
        switch (mode) {
          case RLE:
            setNextNValuesInVector(typeWidth, nullabilityHolder, valuesReader, bufferIdx, vector, numValues);
            bufferIdx += numValues;
            break;
          case PACKED:
            for (int i = 0; i < numValues; ++i) {
              if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
                nextVal(vector, bufferIdx * typeWidth, valuesReader, mode);
                nullabilityHolder.setNotNull(bufferIdx);
                if (setArrowValidityVector) {
                  BitVectorHelper.setBit(vector.getValidityBuffer(), bufferIdx);
                }
              } else {
                setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
              }
              bufferIdx++;
            }
            break;
        }
        left -= numValues;
        currentCount -= numValues;
      }
    }

    public void nextDictEncodedBatch(
        final FieldVector vector, final int startOffset, final int typeWidth,
        final int numValsToRead, NullabilityHolder nullabilityHolder,
        VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader, Dictionary dict) {
      int idx = startOffset;
      int left = numValsToRead;
      while (left > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int numValues = Math.min(left, currentCount);
        ArrowBuf validityBuffer = vector.getValidityBuffer();
        switch (mode) {
          case RLE:
            if (currentValue == maxDefLevel) {
              nextDictEncodedVal(vector, idx, dictionaryEncodedValuesReader, dict, mode,
                  numValues, nullabilityHolder, typeWidth);
            } else {
              setNulls(nullabilityHolder, idx, numValues, validityBuffer);
            }
            idx += numValues;
            break;
          case PACKED:
            for (int i = 0; i < numValues; i++) {
              if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
                nextDictEncodedVal(vector, idx, dictionaryEncodedValuesReader, dict, mode,
                    numValues, nullabilityHolder, typeWidth);
                nullabilityHolder.setNotNull(idx);
                if (setArrowValidityVector) {
                  BitVectorHelper.setBit(vector.getValidityBuffer(), idx);
                }
              } else {
                setNull(nullabilityHolder, idx, validityBuffer);
              }
              idx++;
            }
            break;
        }
        left -= numValues;
        currentCount -= numValues;
      }
    }

    protected abstract void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode);
    protected abstract void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
        Dictionary dict, Mode mode, int numValues, NullabilityHolder holder, int typeWidth);
  }

  class LongReader extends NumericBaseReader {
    @Override
    protected void nextVal(FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setLong(idx, valuesReader.readLong());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
        Dictionary dict, Mode mode, int numValues, NullabilityHolder holder, int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        dictionaryEncodedValuesReader.longDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector.getDataBuffer()
            .setLong((long) idx * typeWidth, dict.decodeToLong(dictionaryEncodedValuesReader.readInteger()));
      }
    }
  }

  class DoubleReader extends NumericBaseReader {
    @Override
    protected void nextVal(FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setDouble(idx, valuesReader.readDouble());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
        Dictionary dict, Mode mode, int numValues, NullabilityHolder holder, int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        dictionaryEncodedValuesReader.doubleDictEncodedReader().nextBatch(vector,
            idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector.getDataBuffer()
            .setDouble((long) idx * typeWidth, dict.decodeToDouble(dictionaryEncodedValuesReader.readInteger()));
      }
    }
  }

  class FloatReader extends NumericBaseReader {
    @Override
    protected void nextVal(FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setFloat(idx, valuesReader.readFloat());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx,
        VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
        Dictionary dict, Mode mode, int numValues, NullabilityHolder holder, int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        dictionaryEncodedValuesReader.floatDictEncodedReader().nextBatch(vector,
            idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector.getDataBuffer()
            .setFloat((long) idx * typeWidth, dict.decodeToFloat(dictionaryEncodedValuesReader.readInteger()));
      }
    }
  }

  class IntegerReader extends NumericBaseReader {
    @Override
    protected void nextVal(FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setInt(idx, valuesReader.readInteger());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx,
        VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
        Dictionary dict, Mode mode, int numValues, NullabilityHolder holder, int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        dictionaryEncodedValuesReader.integerDictEncodedReader().nextBatch(vector,
            idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector.getDataBuffer()
            .setInt((long) idx * typeWidth, dict.decodeToInt(dictionaryEncodedValuesReader.readInteger()));
      }
    }
  }

  abstract class BaseReader {
    public void nextBatch(
        final FieldVector vector, final int startOffset, final int typeWidth,
        final int numValsToRead, NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader) {
      int bufferIdx = startOffset;
      int left = numValsToRead;
      while (left > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int numValues = Math.min(left, currentCount);
        byte[] byteArray = null;
        if (typeWidth > -1) {
          byteArray = new byte[typeWidth];
        }
        switch (mode) {
          case RLE:
            if (currentValue == maxDefLevel) {
              for (int i = 0; i < numValues; i++) {
                nextVal(vector, bufferIdx, valuesReader, typeWidth, byteArray);
                nullabilityHolder.setNotNull(bufferIdx);
                bufferIdx++;
              }
            } else {
              setNulls(nullabilityHolder, bufferIdx, numValues, vector.getValidityBuffer());
              bufferIdx += numValues;
            }
            break;
          case PACKED:
            for (int i = 0; i < numValues; i++) {
              if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
                nextVal(vector, bufferIdx, valuesReader, typeWidth, byteArray);
                nullabilityHolder.setNotNull(bufferIdx);
              } else {
                setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
              }
              bufferIdx++;
            }
            break;
        }
        left -= numValues;
        currentCount -= numValues;
      }
    }

    public void nextDictEncodedBatch(
        final FieldVector vector, final int startOffset, final int typeWidth,
        final int numValsToRead, NullabilityHolder nullabilityHolder,
        VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
        Dictionary dict) {
      int idx = startOffset;
      int left = numValsToRead;
      while (left > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int numValues = Math.min(left, currentCount);
        ArrowBuf validityBuffer = vector.getValidityBuffer();
        switch (mode) {
          case RLE:
            if (currentValue == maxDefLevel) {
              nextDictEncodedVal(vector,
                  idx, dictionaryEncodedValuesReader, numValues, dict, nullabilityHolder, typeWidth, mode);
            } else {
              setNulls(nullabilityHolder, idx, numValues, validityBuffer);
            }
            idx += numValues;
            break;
          case PACKED:
            for (int i = 0; i < numValues; i++) {
              if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
                nextDictEncodedVal(vector,
                    idx, dictionaryEncodedValuesReader, numValues, dict, nullabilityHolder, typeWidth, mode);
                nullabilityHolder.setNotNull(idx);
                if (setArrowValidityVector) {
                  BitVectorHelper.setBit(vector.getValidityBuffer(), idx);
                }
              } else {
                setNull(nullabilityHolder, idx, validityBuffer);
              }
              idx++;
            }
            break;
        }
        left -= numValues;
        currentCount -= numValues;
      }
    }

    protected abstract void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray);
    protected abstract void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode);
  }

  class TimestampMillisReader extends BaseReader {

    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      vector.getDataBuffer().setLong((long) idx * typeWidth, valuesReader.readLong() * 1000);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.timestampMillisDictEncodedReader().nextBatch(vector,
            idx, numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector.getDataBuffer().setLong((long) idx * typeWidth, dict.decodeToLong(reader.readInteger()) * 1000);
      }
    }
  }

  class FixedWidthBinaryReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
      ((VarBinaryVector) vector).setSafe(idx, buffer.array(), buffer.position() + buffer.arrayOffset(),
          buffer.limit() - buffer.position());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.fixedWidthBinaryDictEncodedReader()
            .nextBatch(vector, idx, numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ByteBuffer buffer = dict.decodeToBinary(reader.readInteger()).toByteBuffer();
        vector.getDataBuffer().setBytes(
            (long) idx * typeWidth, buffer.array(),
            buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
      }
    }
  }

  class FixedLengthDecimalReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      valuesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
      ((DecimalVector) vector).setBigEndian(idx, byteArray);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.fixedLengthDecimalDictEncodedReader()
            .nextBatch(vector, idx, numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ByteBuffer decimalBytes = dict.decodeToBinary(reader.readInteger()).toByteBuffer();
        byte[] vectorBytes = new byte[typeWidth];
        System.arraycopy(decimalBytes, 0, vectorBytes, 0, typeWidth);
        ((DecimalVector) vector).setBigEndian(idx, vectorBytes);
      }
    }
  }

  class FixedSizeBinaryReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      valuesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
      ((FixedSizeBinaryVector) vector).set(idx, byteArray);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.fixedSizeBinaryDictEncodedReader().nextBatch(vector, idx,
            numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        byte[] bytes = dict.decodeToBinary(reader.readInteger()).getBytes();
        byte[] vectorBytes = new byte[typeWidth];
        System.arraycopy(bytes, 0, vectorBytes, 0, typeWidth);
        ((FixedSizeBinaryVector) vector).set(idx, vectorBytes);
      }
    }
  }

  class VarWidthReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      int len = valuesReader.readInteger();
      ByteBuffer buffer = valuesReader.getBuffer(len);
      // Calling setValueLengthSafe takes care of allocating a larger buffer if
      // running out of space.
      ((BaseVariableWidthVector) vector).setValueLengthSafe(idx, len);
      int startOffset = ((BaseVariableWidthVector) vector).getStartOffset(idx);
      // It is possible that the data buffer was reallocated. So it is important to
      // not cache the data buffer reference but instead use vector.getDataBuffer().
      vector.getDataBuffer().setBytes(startOffset, buffer.array(), buffer.position() + buffer.arrayOffset(),
          buffer.limit() - buffer.position());
      // Similarly, we need to get the latest reference to the validity buffer as well
      // since reallocation changes reference of the validity buffers as well.
      if (setArrowValidityVector) {
        BitVectorHelper.setBit(vector.getValidityBuffer(), idx);
      }
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.varWidthBinaryDictEncodedReader().nextBatch(vector, idx,
            numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ((BaseVariableWidthVector) vector).setSafe(
            idx, dict.decodeToBinary(reader.readInteger()).getBytesUnsafe());
      }
    }
  }

  class IntBackedDecimalReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      ((DecimalVector) vector).set(idx, valuesReader.getBuffer(Integer.BYTES).getInt());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.intBackedDecimalDictEncodedReader()
            .nextBatch(vector, idx, numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ((DecimalVector) vector).set(idx, dict.decodeToInt(reader.readInteger()));
      }
    }
  }

  class LongBackedDecimalReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      ((DecimalVector) vector).set(idx, valuesReader.getBuffer(Long.BYTES).getLong());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.longBackedDecimalDictEncodedReader()
            .nextBatch(vector, idx, numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ((DecimalVector) vector).set(
            idx, dict.decodeToLong(reader.readInteger()));
      }
    }
  }

  class BooleanReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      ((BitVector) vector).setSafe(idx, valuesReader.readBooleanAsInt());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      throw new UnsupportedOperationException();
    }
  }

  class DictionaryIdReader extends BaseReader {

    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, int typeWidth, byte[] byteArray) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector, int idx, VectorizedDictionaryEncodedParquetValuesReader reader,
        int numValuesToRead, Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth, Mode mode) {
      if (Mode.RLE.equals(mode)) {
        reader.dictionaryIdReader().nextBatch(vector, idx, numValuesToRead, dict, nullabilityHolder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector.getDataBuffer().setInt((long) idx * IntVector.TYPE_WIDTH, reader.readInteger());
      }
    }
  }

  private void setNull(NullabilityHolder nullabilityHolder, int bufferIdx, ArrowBuf validityBuffer) {
    nullabilityHolder.setNull(bufferIdx);
    if (setArrowValidityVector) {
      BitVectorHelper.setValidityBit(validityBuffer, bufferIdx, 0);
    }
  }

  private void setNulls(NullabilityHolder nullabilityHolder, int idx, int numValues, ArrowBuf validityBuffer) {
    nullabilityHolder.setNulls(idx, numValues);
    if (setArrowValidityVector) {
      for (int i = 0; i < numValues; i++) {
        BitVectorHelper.setValidityBit(validityBuffer, idx + i, 0);
      }
    }
  }

  private void setNextNValuesInVector(
      int typeWidth, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader, int bufferIdx, FieldVector vector, int numValues) {
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    if (currentValue == maxDefLevel) {
      ByteBuffer buffer = valuesReader.getBuffer(numValues * typeWidth);
      vector.getDataBuffer().setBytes((long) bufferIdx * typeWidth, buffer);
      nullabilityHolder.setNotNulls(bufferIdx, numValues);
      if (setArrowValidityVector) {
        for (int i = 0; i < numValues; i++) {
          BitVectorHelper.setBit(validityBuffer, bufferIdx + i);
        }
      }
    } else {
      setNulls(nullabilityHolder, bufferIdx, numValues, validityBuffer);
    }
  }

  LongReader longReader() {
    return new LongReader();
  }

  DoubleReader doubleReader() {
    return new DoubleReader();
  }

  FloatReader floatReader() {
    return new FloatReader();
  }

  IntegerReader integerReader() {
    return new IntegerReader();
  }

  TimestampMillisReader timestampMillisReader() {
    return new TimestampMillisReader();
  }

  FixedWidthBinaryReader fixedWidthBinaryReader() {
    return new FixedWidthBinaryReader();
  }

  FixedLengthDecimalReader fixedLengthDecimalReader() {
    return new FixedLengthDecimalReader();
  }

  FixedSizeBinaryReader fixedSizeBinaryReader() {
    return new FixedSizeBinaryReader();
  }

  VarWidthReader varWidthReader() {
    return new VarWidthReader();
  }

  IntBackedDecimalReader intBackedDecimalReader() {
    return new IntBackedDecimalReader();
  }

  LongBackedDecimalReader longBackedDecimalReader() {
    return new LongBackedDecimalReader();
  }

  BooleanReader booleanReader() {
    return new BooleanReader();
  }

  DictionaryIdReader dictionaryIdReader() {
    return new DictionaryIdReader();
  }
}
