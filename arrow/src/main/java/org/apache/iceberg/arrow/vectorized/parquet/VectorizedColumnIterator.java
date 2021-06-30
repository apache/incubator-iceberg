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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.BaseColumnIterator;
import org.apache.iceberg.parquet.BasePageIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.PageReader;

/**
 * Vectorized version of the ColumnIterator that reads column values in data pages of a column in a row group in a
 * batched fashion.
 */
public class VectorizedColumnIterator extends BaseColumnIterator {

  private final VectorizedPageIterator vectorizedPageIterator;
  private int batchSize;

  public VectorizedColumnIterator(ColumnDescriptor desc, String writerVersion, boolean setArrowValidityVector) {
    super(desc);
    Preconditions.checkArgument(
        desc.getMaxRepetitionLevel() == 0,
        "Only non-nested columns are supported for vectorized reads");
    this.vectorizedPageIterator = new VectorizedPageIterator(desc, writerVersion, setArrowValidityVector);
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public Dictionary setRowGroupInfo(PageReader store, boolean allPagesDictEncoded) {
    // setPageSource can result in a data page read. If that happens, we need
    // to know in advance whether all the pages in the row group are dictionary encoded or not
    this.vectorizedPageIterator.setAllPagesDictEncoded(allPagesDictEncoded);
    super.setPageSource(store);
    return dictionary;
  }

  public void nextBatchIntegers(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchIntegers(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchDictionaryIds(IntVector vector, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchDictionaryIds(vector, batchSize - rowsReadSoFar,
          rowsReadSoFar, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      vector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchLongs(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchLongs(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchTimestampMillis(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchTimestampMillis(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchFloats(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchFloats(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchDoubles(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchDoubles(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchIntBackedDecimal(
      FieldVector fieldVector,
      NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchIntBackedDecimal(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchLongBackedDecimal(
      FieldVector fieldVector,
      NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchLongBackedDecimal(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchFixedLengthDecimal(
      FieldVector fieldVector,
      int typeWidth,
      NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchFixedLengthDecimal(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, typeWidth, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchFixedSizeBinary(
      FieldVector fieldVector,
      int typeWidth,
      NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchFixedSizeBinary(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, typeWidth, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchVarWidthType(FieldVector fieldVector, NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchVarWidthType(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchFixedWidthBinary(FieldVector fieldVector, int typeWidth, NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchFixedWidthBinary(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, typeWidth, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchBoolean(FieldVector fieldVector, NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchBoolean(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  @Override
  protected BasePageIterator pageIterator() {
    return vectorizedPageIterator;
  }

  public boolean producesDictionaryEncodedVector() {
    return vectorizedPageIterator.producesDictionaryEncodedVector();
  }
}
