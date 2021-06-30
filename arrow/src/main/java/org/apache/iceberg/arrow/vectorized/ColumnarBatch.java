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

import java.util.Arrays;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This class is inspired by Spark's {@code ColumnarBatch}. This class wraps a columnar batch in the result set of an
 * Iceberg table query.
 */
public class ColumnarBatch implements AutoCloseable {

  private final int numRows;
  private final ColumnVector[] columns;

  ColumnarBatch(int numRows, ColumnVector[] columns) {
    for (int i = 0; i < columns.length; i++) {
      int columnValueCount = columns[i].getFieldVector().getValueCount();
      Preconditions.checkArgument(
          numRows == columnValueCount,
          "Number of rows (=" + numRows + ") != column[" + i + "] size (=" + columnValueCount + ")");
    }
    this.numRows = numRows;
    this.columns = columns;
  }

  /**
   * Create a new instance of {@link VectorSchemaRoot} from the arrow vectors stored in this arrow batch. The arrow
   * vectors are owned by the reader.
   */
  public VectorSchemaRoot createVectorSchemaRootFromVectors() {
    return VectorSchemaRoot.of(Arrays.stream(columns)
        .map(ColumnVector::getFieldVector)
        .toArray(FieldVector[]::new));
  }

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after calling this. This must be
   * called at the end to clean up memory allocations.
   */
  @Override
  public void close() {
    for (ColumnVector c : columns) {
      c.close();
    }
  }

  /**
   * Returns the number of columns that make up this batch.
   */
  public int numCols() {
    return columns.length;
  }

  /**
   * Returns the number of rows for read, including filtered rows.
   */
  public int numRows() {
    return numRows;
  }

  /**
   * Returns the column at `ordinal`.
   */
  public ColumnVector column(int ordinal) {
    return columns[ordinal];
  }
}
