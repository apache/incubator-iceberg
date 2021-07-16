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

package org.apache.iceberg.flink.data;

import java.util.Iterator;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.junit.Assert;
import org.junit.Test;

public class TestRowDataProjection {

  @Test
  public void testFullProjection() {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    generateAndValidate(schema, schema);
  }

  @Test
  public void testReorderedFullProjection() {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Schema reordered = new Schema(
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    generateAndValidate(schema, reordered);
  }

  @Test
  public void testBasicProjection() {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );
    Schema id = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );
    Schema data = new Schema(
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );
    generateAndValidate(schema, id);
    generateAndValidate(schema, data);
  }

  @Test
  public void testEmptyProjection() {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );
    generateAndValidate(schema, schema.select());
  }

  @Test
  public void testRename() {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Schema renamed = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "renamed", Types.StringType.get())
    );
    generateAndValidate(schema, renamed);
  }

  @Test
  public void testNestedProjection() {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    // Project id only.
    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );
    generateAndValidate(schema, idOnly);

    // Project lat only.
    Schema latOnly = new Schema(
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get())
        ))
    );
    generateAndValidate(schema, latOnly);

    // Project long only.
    Schema longOnly = new Schema(
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );
    generateAndValidate(schema, longOnly);

    // Project location.
    Schema locationOnly = schema.select("location");
    generateAndValidate(schema, locationOnly);
  }

  private void generateAndValidate(Schema schema, Schema projectSchema) {
    int numRecords = 100;
    Iterable<Record> recordList = RandomGenericData.generate(schema, numRecords, 102L);
    Iterable<RowData> rowDataList = RandomRowData.generate(schema, numRecords, 102L);

    StructProjection structProjection = StructProjection.create(schema, projectSchema);
    RowDataProjection rowDataProjection = RowDataProjection.create(schema, projectSchema);

    Iterator<Record> recordIter = recordList.iterator();
    Iterator<RowData> rowDataIter = rowDataList.iterator();

    for (int i = 0; i < numRecords; i++) {
      Assert.assertTrue("Should have more records", recordIter.hasNext());
      Assert.assertTrue("Should have more RowData", rowDataIter.hasNext());

      StructLike expected = structProjection.wrap(recordIter.next());
      RowData actual = rowDataProjection.project(rowDataIter.next());

      TestHelpers.assertRowData(projectSchema, expected, actual);
    }

    Assert.assertFalse("Shouldn't have more record", recordIter.hasNext());
    Assert.assertFalse("Shouldn't have more RowData", rowDataIter.hasNext());
  }
}
