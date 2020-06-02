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

import java.util.List;
import java.util.stream.Stream;

public abstract class MetricsCollectorBase<D> implements MetricsCollector<D> {

  private List<MetricsCollector> collectors = null;
  private Integer id;

  public MetricsCollectorBase() { }

  public MetricsCollectorBase(List<MetricsCollector> collectors) {
    this.id = null;
    this.collectors = collectors;
  }

  public MetricsCollectorBase(int id, List<MetricsCollector> collectors) {
    this.id = id;
    this.collectors = collectors;
  }

  @Override
  public Stream<FieldMetrics> getMetrics() {
    return collectors.stream().flatMap(MetricsCollector::getMetrics);
  }

  @Override
  public Long count() {
    throw new UnsupportedOperationException("count() only implemented for root metrics collector");
  }
}
