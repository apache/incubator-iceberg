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
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.TestAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;

public class TestGenericAppenderFactory extends TestAppenderFactory<Record> {

  private final GenericRecord gRecord;

  public TestGenericAppenderFactory(String fileFormat, boolean partitioned) {
    super(fileFormat, partitioned);
    this.gRecord = GenericRecord.create(SCHEMA);
  }

  @Override
  protected FileAppenderFactory<Record> createAppenderFactory(List<Integer> equalityFieldIds,
                                                              Schema eqDeleteSchema,
                                                              Schema posDeleteRowSchema) {
    return new GenericAppenderFactory(table,
        ArrayUtil.toIntArray(equalityFieldIds), eqDeleteSchema, posDeleteRowSchema);
  }

  @Override
  protected Record createRow(Integer id, String data) {
    return gRecord.copy(ImmutableMap.of("id", id, "data", data));
  }

  @Override
  protected StructLikeSet expectedRowSet(Iterable<Record> records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.forEach(set::add);
    return set;
  }
}
