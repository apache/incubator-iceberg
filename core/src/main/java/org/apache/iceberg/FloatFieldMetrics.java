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
 * Iceberg internally tracked field level metrics, used by Parquet and ORC writers only.
 * <p>
 * Parquet/ORC keeps track of most metrics in file statistics, and only NaN counter is actually tracked by writers.
 * This wrapper ensures that metrics not being updated by those writers will not be incorrectly used, by throwing
 * exceptions when they are accessed.
 */
public class FloatFieldMetrics extends FieldMetrics<Number> {

  /**
   * Constructor for creating a FieldMetrics with only NaN counter.
   * @param id field id being tracked by the writer
   * @param nanValueCount number of NaN values, will only be non-0 for double or float field.
   */
  public FloatFieldMetrics(int id,
                           long nanValueCount) {
    super(id, 0L, 0L, nanValueCount, null, null);
  }

  @Override
  public long valueCount() {
    throw new IllegalStateException("Shouldn't access this method, as this metric is tracked in file statistics. ");
  }

  @Override
  public long nullValueCount() {
    throw new IllegalStateException("Shouldn't access this method, as this metric is tracked in file statistics. ");
  }

  @Override
  public Number lowerBound() {
    throw new IllegalStateException("Shouldn't access this method, as this metric is tracked in file statistics. ");
  }

  @Override
  public Number upperBound() {
    throw new IllegalStateException("Shouldn't access this method, as this metric is tracked in file statistics. ");
  }
}
