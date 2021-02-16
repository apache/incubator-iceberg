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

package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.TestAppenderFactory;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class TestSparkAppenderFactory extends TestAppenderFactory<InternalRow> {

  private final StructType sparkType;

  public TestSparkAppenderFactory(String fileFormat, boolean partitioned) {
    super(fileFormat, partitioned);
    this.sparkType = SparkSchemaUtil.convert(SCHEMA);
  }

  @Override
  protected FileAppenderFactory<InternalRow> createAppenderFactory(List<Integer> equalityFieldIds,
                                                                   Schema eqDeleteSchema,
                                                                   Schema posDeleteRowSchema) {
<<<<<<< HEAD
    return SparkAppenderFactory.builderFor(table, table.schema(), sparkType)
        .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
        .eqDeleteRowSchema(eqDeleteSchema)
        .posDelRowSchema(posDeleteRowSchema).build();
=======
    return SparkAppenderFactory.builderFor(table.properties(), table.schema(), sparkType)
      .partitionSpec(table.spec())
      .sortOrder(table.sortOrder())
      .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
      .eqDeleteRowSchema(eqDeleteSchema)
      .posDelRowSchema(posDeleteRowSchema)
      .build();
>>>>>>> Create SparkAppenderFactory builder, avoid argument explosion
  }

  @Override
  protected InternalRow createRow(Integer id, String data) {
    InternalRow row = new GenericInternalRow(2);
    row.update(0, id);
    row.update(1, UTF8String.fromString(data));
    return row;
  }

  @Override
  protected StructLikeSet expectedRowSet(Iterable<InternalRow> rows) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    for (InternalRow row : rows) {
      InternalRowWrapper wrapper = new InternalRowWrapper(sparkType);
      set.add(wrapper.wrap(row));
    }
    return set;
  }
}
