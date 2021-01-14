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

package org.apache.iceberg;


/**
 * Iceberg internally tracked field level metrics.
 */
public class FieldMetrics {
  private final int id;
  private final long valueCount;
  private final long nullValueCount;
  private final long nanValueCount;
  private final Object lowerBound;
  private final Object upperBound;

  public FieldMetrics(int id,
                      long valueCount,
                      long nullValueCount,
                      long nanValueCount,
                      Object lowerBound,
                      Object upperBound) {
    this.id = id;
    this.valueCount = valueCount;
    this.nullValueCount = nullValueCount;
    this.nanValueCount = nanValueCount;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  /**
   * Returns the id of the field that the metrics within this class are associated with.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the number of all values, including nulls, NaN and repeated, for the given field.
   */
  public long valueCount() {
    return valueCount;
  }

  /**
   * Returns the number of null values for this field.
   */
  public long nullValueCount() {
    return nullValueCount;
  }

  /**
   * Returns the number of NaN values for this field. Will only be non-0 if this field is a double or float field.
   */
  public long nanValueCount() {
    return nanValueCount;
  }

  /**
   * Returns the lower bound value of this field.
   */
  public Object lowerBound() {
    return lowerBound;
  }

  /**
   * Returns the upper bound value of this field.
   */
  public Object upperBound() {
    return upperBound;
  }
}
