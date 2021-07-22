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

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestPathIdentifier extends SparkTestBase {

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      required(2, "data", Types.StringType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableLocation;
  private PathIdentifier identifier;
  private SparkCatalog sparkCatalog;

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    identifier = new PathIdentifier(tableLocation.getAbsolutePath(), null, null);
    sparkCatalog = new SparkCatalog();
    sparkCatalog.initialize("test", new CaseInsensitiveStringMap(ImmutableMap.of()));
  }

  @After
  public void after() {
    tableLocation.delete();
    sparkCatalog = null;
  }

  @Test
  public void testPathIdentifier() throws TableAlreadyExistsException, NoSuchTableException {
    SparkTable table = sparkCatalog.createTable(identifier,
        SparkSchemaUtil.convert(SCHEMA),
        new Transform[0],
        ImmutableMap.of());

    Assert.assertEquals(table.table().location(), tableLocation.getAbsolutePath());
    Assertions.assertThat(table.table()).isInstanceOf(BaseTable.class);
    Assertions.assertThat(((BaseTable) table.table()).operations()).isInstanceOf(HadoopTableOperations.class);

    Assert.assertEquals(sparkCatalog.loadTable(identifier), table);
    Assert.assertTrue(sparkCatalog.dropTable(identifier));
  }
}

