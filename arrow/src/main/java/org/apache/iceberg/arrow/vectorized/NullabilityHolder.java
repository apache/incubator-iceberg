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

/**
 * Instances of this class simply track whether a value at an index is null. For simplicity and performance, it is
 * expected that various setter methods {@link #setNull(int)}, {@link #setNulls(int, int)}, {@link #setNotNull(int)} and
 * {@link #setNotNulls(int, int)} are invoked with monotonically increasing values for the index parameter.
 */
public class NullabilityHolder {
  private final byte[] isNull;
  private int numNulls;
  private final byte[] nonNulls;
  private final byte[] nulls;

  public NullabilityHolder(int size) {
    this.isNull = new byte[size];
    this.nonNulls = new byte[size];
    Arrays.fill(nonNulls, (byte) 0);
    this.nulls = new byte[size];
    Arrays.fill(nulls, (byte) 1);
  }

  public void setNull(int index) {
    isNull[index] = 1;
    numNulls++;
  }

  public void setNotNull(int index) {
    isNull[index] = 0;
  }

  public void setNulls(int startIndex, int num) {
    System.arraycopy(nulls, 0, isNull, startIndex, num);
  }

  public void setNotNulls(int startIndex, int num) {
    System.arraycopy(nonNulls, 0, isNull, startIndex, num);
  }

  /**
   * Returns 1 if null, 0 otherwise.
   */
  public byte isNullAt(int index) {
    return isNull[index];
  }

  public boolean hasNulls() {
    return numNulls > 0;
  }

  public int numNulls() {
    return numNulls;
  }

  public void reset() {
    numNulls = 0;
  }
}
