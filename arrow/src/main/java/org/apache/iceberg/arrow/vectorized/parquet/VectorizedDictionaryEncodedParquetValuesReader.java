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
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.parquet.column.Dictionary;

/**
 * This decoder reads Parquet dictionary encoded data in a vectorized fashion. Unlike other vectorized readers, methods
 * in this decoder don't need to read definition levels. In other words, these methods are called when there are
 * non-null values to be read.
 */
public class VectorizedDictionaryEncodedParquetValuesReader extends BaseVectorizedParquetValuesReader {

  public VectorizedDictionaryEncodedParquetValuesReader(int maxDefLevel, boolean setValidityVector) {
    super(maxDefLevel, setValidityVector);
  }

  void readBatchOfDictionaryIds(
      IntVector intVector, int startOffset, int numValuesToRead,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < numValues; i++) {
            intVector.set(idx, currentValue);
            setNotNull(intVector, nullabilityHolder, idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            intVector.set(idx, packedValuesBuffer[packedValuesBufferIdx++]);
            setNotNull(intVector, nullabilityHolder, idx);
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  void readBatchOfDictionaryEncodedLongs(
      FieldVector vector, int startOffset, int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder, int typeWidth) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < numValues; i++) {
            vector.getDataBuffer().setLong(idx * typeWidth, dict.decodeToLong(currentValue));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            vector.getDataBuffer()
                .setLong(idx * typeWidth, dict.decodeToLong(packedValuesBuffer[packedValuesBufferIdx++]));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  void readBatchOfDictionaryEncodedTimestampMillis(
      FieldVector vector, int startOffset, int numValuesToRead,
      Dictionary dict, NullabilityHolder nullabilityHolder, int typeWidth) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < numValues; i++) {
            vector.getDataBuffer().setLong(idx * typeWidth, dict.decodeToLong(currentValue) * 1000);
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            vector.getDataBuffer()
                .setLong(idx * typeWidth, dict.decodeToLong(packedValuesBuffer[packedValuesBufferIdx++]) * 1000);
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  void readBatchOfDictionaryEncodedIntegers(
      FieldVector vector, int startOffset, int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder, int typeWidth) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer().setInt(idx * typeWidth, dict.decodeToInt(currentValue));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer()
                .setInt(idx * typeWidth, dict.decodeToInt(packedValuesBuffer[packedValuesBufferIdx++]));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedFloats(
      FieldVector vector, int startOffset, int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder, int typeWidth) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer().setFloat(idx * typeWidth, dict.decodeToFloat(currentValue));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer()
                .setFloat(idx * typeWidth, dict.decodeToFloat(packedValuesBuffer[packedValuesBufferIdx++]));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedDoubles(
      FieldVector vector, int startOffset, int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder, int typeWidth) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer().setDouble(idx * typeWidth, dict.decodeToDouble(currentValue));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer()
                .setDouble(idx * typeWidth, dict.decodeToDouble(packedValuesBuffer[packedValuesBufferIdx++]));
            setNotNull(vector, nullabilityHolder, idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedFixedWidthBinary(
      FieldVector vector, int typeWidth, int startOffset,
      int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(currentValue).toByteBuffer();
            setFixedWidthBinary(vector, typeWidth, nullabilityHolder, idx, buffer);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(packedValuesBuffer[packedValuesBufferIdx++]).toByteBuffer();
            setFixedWidthBinary(vector, typeWidth, nullabilityHolder, idx, buffer);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  private void setFixedWidthBinary(
      FieldVector vector, int typeWidth, NullabilityHolder nullabilityHolder,
      int idx, ByteBuffer buffer) {
    vector.getDataBuffer()
        .setBytes(idx * typeWidth, buffer.array(),
            buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
    setNotNull(vector, nullabilityHolder, idx);
  }

  private void setNotNull(FieldVector vector, NullabilityHolder nullabilityHolder, int idx) {
    nullabilityHolder.setNotNull(idx);
    if (setArrowValidityVector) {
      BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
    }
  }

  void readBatchOfDictionaryEncodedFixedLengthDecimals(
      FieldVector vector, int typeWidth, int startOffset,
      int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            byte[] decimalBytes = dict.decodeToBinary(currentValue).getBytesUnsafe();
            byte[] vectorBytes = new byte[typeWidth];
            System.arraycopy(decimalBytes, 0, vectorBytes, 0, typeWidth);
            ((DecimalVector) vector).setBigEndian(idx, vectorBytes);
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            byte[] decimalBytes = dict.decodeToBinary(packedValuesBuffer[packedValuesBufferIdx++]).getBytesUnsafe();
            byte[] vectorBytes = new byte[typeWidth];
            System.arraycopy(decimalBytes, 0, vectorBytes, 0, typeWidth);
            ((DecimalVector) vector).setBigEndian(idx, vectorBytes);
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedVarWidthBinary(
      FieldVector vector, int startOffset, int numValuesToRead,
      Dictionary dict, NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(currentValue).toByteBuffer();
            ((BaseVariableWidthVector) vector).setSafe(idx, buffer.array(),
                buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(packedValuesBuffer[packedValuesBufferIdx++]).toByteBuffer();
            ((BaseVariableWidthVector) vector).setSafe(idx, buffer.array(),
                buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedIntBackedDecimals(
      FieldVector vector, int startOffset,
      int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            ((DecimalVector) vector).set(
                idx,
                dict.decodeToInt(currentValue));
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            ((DecimalVector) vector).set(
                idx, dict.decodeToInt(packedValuesBuffer[packedValuesBufferIdx++]));
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedLongBackedDecimals(
      FieldVector vector, int startOffset,
      int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            ((DecimalVector) vector).set(
                idx,
                dict.decodeToLong(currentValue));
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            ((DecimalVector) vector).set(
                idx, dict.decodeToLong(packedValuesBuffer[packedValuesBufferIdx++]));
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedFixedSizeBinary(
      FieldVector vector, int typeWidth, int startOffset,
      int numValuesToRead, Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = startOffset;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            byte[] bytes = dict.decodeToBinary(currentValue).getBytesUnsafe();
            byte[] vectorBytes = new byte[typeWidth];
            System.arraycopy(bytes, 0, vectorBytes, 0, typeWidth);
            ((FixedSizeBinaryVector) vector).set(idx, vectorBytes);
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            byte[] decimalBytes = dict.decodeToBinary(packedValuesBuffer[packedValuesBufferIdx++]).getBytesUnsafe();
            byte[] vectorBytes = new byte[typeWidth];
            System.arraycopy(decimalBytes, 0, vectorBytes, 0, typeWidth);
            ((FixedSizeBinaryVector) vector).set(idx, vectorBytes);
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }
}
