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

import java.io.IOException;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

public class VectorizedPageIterator extends BasePageIterator {
  private final boolean setArrowValidityVector;

  public VectorizedPageIterator(ColumnDescriptor desc, String writerVersion, boolean setValidityVector) {
    super(desc, writerVersion);
    this.setArrowValidityVector = setValidityVector;
  }

  private ValuesAsBytesReader plainValuesReader = null;
  private VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader = null;
  private boolean allPagesDictEncoded;
  private VectorizedParquetDefinitionLevelReader vectorizedDefinitionLevelReader;

  private enum DictionaryDecodeMode {
    NONE, // plain encoding
    LAZY,
    EAGER
  }

  private DictionaryDecodeMode dictionaryDecodeMode;

  public void setAllPagesDictEncoded(boolean allDictEncoded) {
    this.allPagesDictEncoded = allDictEncoded;
  }

  @Override
  protected void reset() {
    super.reset();
    this.plainValuesReader = null;
    this.vectorizedDefinitionLevelReader = null;
  }

  /**
   * Method for reading a batch of dictionary ids from the dicitonary encoded data pages. Like definition levels,
   * dictionary ids in Parquet are RLE/bin-packed encoded as well.
   */
  public int nextBatchDictionaryIds(
      final IntVector vector, final int expectedBatchSize,
      final int numValsInVector,
      NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    vectorizedDefinitionLevelReader.readBatchOfDictionaryIds(
        vector,
        numValsInVector,
        actualBatchSize,
        holder,
        dictionaryEncodedValuesReader);
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of INT32 data type
   */
  public int nextBatchIntegers(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedIntegers(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfIntegers(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of INT64 data type
   */
  public int nextBatchLongs(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedLongs(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfLongs(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of TIMESTAMP_MILLIS data type. In iceberg, TIMESTAMP is always represented in
   * micro-seconds. So we multiply values stored in millis with 1000 before writing them to the vector.
   */
  public int nextBatchTimestampMillis(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedTimestampMillis(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfTimestampMillis(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of FLOAT data type.
   */
  public int nextBatchFloats(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedFloats(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfFloats(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of values of DOUBLE data type
   */
  public int nextBatchDoubles(
      final FieldVector vector, final int expectedBatchSize,
      final int numValsInVector,
      final int typeWidth, NullabilityHolder holder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedDoubles(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfDoubles(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          holder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  private int getActualBatchSize(int expectedBatchSize) {
    return Math.min(expectedBatchSize, triplesCount - triplesRead);
  }

  /**
   * Method for reading a batch of decimals backed by INT32 and INT64 parquet data types. Since Arrow stores all
   * decimals in 16 bytes, byte arrays are appropriately padded before being written to Arrow data buffers.
   */
  public int nextBatchIntBackedDecimal(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader
          .readBatchOfDictionaryEncodedIntBackedDecimals(
              vector,
              numValsInVector,
              actualBatchSize,
              nullabilityHolder,
              dictionaryEncodedValuesReader,
              dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfIntBackedDecimals(
          vector,
          numValsInVector,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  public int nextBatchLongBackedDecimal(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader
          .readBatchOfDictionaryEncodedLongBackedDecimals(
              vector,
              numValsInVector,
              actualBatchSize,
              nullabilityHolder,
              dictionaryEncodedValuesReader,
              dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfLongBackedDecimals(
          vector,
          numValsInVector,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of decimals backed by fixed length byte array parquet data type. Arrow stores all
   * decimals in 16 bytes. This method provides the necessary padding to the decimals read. Moreover, Arrow interprets
   * the decimals in Arrow buffer as little endian. Parquet stores fixed length decimals as big endian. So, this method
   * uses {@link DecimalVector#setBigEndian(int, byte[])} method so that the data in Arrow vector is indeed little
   * endian.
   */
  public int nextBatchFixedLengthDecimal(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      final int typeWidth, NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedFixedLengthDecimals(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfFixedLengthDecimals(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  public int nextBatchFixedSizeBinary(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      final int typeWidth, NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedFixedSizeBinary(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfFixedSizeBinary(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading a batch of variable width data type (ENUM, JSON, UTF8, BSON).
   */
  public int nextBatchVarWidthType(
      final FieldVector vector,
      final int expectedBatchSize,
      final int numValsInVector,
      NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedVarWidth(
          vector,
          numValsInVector,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchVarWidth(
          vector,
          numValsInVector,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  /**
   * Method for reading batches of fixed width binary type (e.g. BYTE[7]). Spark does not support fixed width binary
   * data type. To work around this limitation, the data is read as fixed width binary from parquet and stored in a
   * {@link VarBinaryVector} in Arrow.
   */
  public int nextBatchFixedWidthBinary(
      final FieldVector vector, final int expectedBatchSize, final int numValsInVector,
      final int typeWidth, NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    if (dictionaryDecodeMode == DictionaryDecodeMode.EAGER) {
      vectorizedDefinitionLevelReader.readBatchOfDictionaryEncodedFixedWidthBinary(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          dictionaryEncodedValuesReader,
          dictionary);
    } else {
      vectorizedDefinitionLevelReader.readBatchOfFixedWidthBinary(
          vector,
          numValsInVector,
          typeWidth,
          actualBatchSize,
          nullabilityHolder,
          plainValuesReader);
    }
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  public boolean producesDictionaryEncodedVector() {
    return dictionaryDecodeMode == DictionaryDecodeMode.LAZY;
  }

  /**
   * Method for reading batches of booleans.
   */
  public int nextBatchBoolean(
      final FieldVector vector,
      final int expectedBatchSize,
      final int numValsInVector,
      NullabilityHolder nullabilityHolder) {
    final int actualBatchSize = getActualBatchSize(expectedBatchSize);
    if (actualBatchSize <= 0) {
      return 0;
    }
    vectorizedDefinitionLevelReader
        .readBatchOfBooleans(vector, numValsInVector, actualBatchSize,
            nullabilityHolder, plainValuesReader);
    triplesRead += actualBatchSize;
    this.hasNext = triplesRead < triplesCount;
    return actualBatchSize;
  }

  @Override
  protected void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = plainValuesReader;
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col " + desc + " as the dictionary was missing for encoding " + dataEncoding);
      }
      try {
        dictionaryEncodedValuesReader =
            new VectorizedDictionaryEncodedParquetValuesReader(desc.getMaxDefinitionLevel(), setArrowValidityVector);
        dictionaryEncodedValuesReader.initFromPage(valueCount, in);
        if (ParquetUtil.isIntType(desc.getPrimitiveType()) || !allPagesDictEncoded) {
          dictionaryDecodeMode = DictionaryDecodeMode.EAGER;
        } else {
          dictionaryDecodeMode = DictionaryDecodeMode.LAZY;
        }
      } catch (IOException e) {
        throw new ParquetDecodingException("could not read page in col " + desc, e);
      }
    } else {
      plainValuesReader = new ValuesAsBytesReader();
      plainValuesReader.initFromPage(valueCount, in);
      dictionaryDecodeMode = DictionaryDecodeMode.NONE;
    }
    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding) &&
        previousReader != null && previousReader instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) plainValuesReader).setPreviousReader(previousReader);
    }
  }

  @Override
  protected void initDefinitionLevelsReader(
      DataPageV1 dataPageV1, ColumnDescriptor desc, ByteBufferInputStream in,
      int triplesCount) throws IOException {
    this.vectorizedDefinitionLevelReader = newVectorizedDefinitionLevelReader(desc);
    this.vectorizedDefinitionLevelReader.initFromPage(triplesCount, in);
  }

  @Override
  protected void initDefinitionLevelsReader(DataPageV2 dataPageV2, ColumnDescriptor desc) {
    this.vectorizedDefinitionLevelReader = newVectorizedDefinitionLevelReader(desc);
  }

  private VectorizedParquetDefinitionLevelReader newVectorizedDefinitionLevelReader(ColumnDescriptor desc) {
    int bitwidth = BytesUtils.getWidthFromMaxInt(desc.getMaxDefinitionLevel());
    return new VectorizedParquetDefinitionLevelReader(bitwidth, desc.getMaxDefinitionLevel(), setArrowValidityVector);
  }
}
