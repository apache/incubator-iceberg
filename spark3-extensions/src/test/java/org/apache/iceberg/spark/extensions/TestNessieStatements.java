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

package org.apache.iceberg.spark.extensions;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Tag;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestNessieStatements extends SparkTestBase {

  @ClassRule
  @Rule
  public static TemporaryFolder temp = new TemporaryFolder();
  private static NessieClient client;

  @BeforeClass
  public static void startMetastoreAndSpark()  {
    File tempFile;
    try {
      tempFile = temp.newFolder();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String port = System.getProperty("quarkus.http.test-port", "19120");
    String path = String.format("http://localhost:%s/api/v1", port);
    Map<String, String> nessieParams = ImmutableMap.of("ref", "main", "url", path, "warehouse",
        tempFile.toURI().toString());

    SparkSession.Builder builder = SparkSession.builder()
        .master("local[2]")
        .config("spark.testing", "true")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.catalog.nessie.catalog-impl", NessieCatalog.class.getName())
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog");
    nessieParams.forEach((k, v) -> builder.config(String.format("spark.sql.catalog.nessie.%s", k), v));

    SparkTestBase.spark = builder.getOrCreate();

    client = NessieClient.none(path);

  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    spark.stop();
    SparkTestBase.spark = null;
  }

  @Test
  public void testCreateBranch() {
    String hash;
    try {
      hash = client.getTreeApi().getDefaultBranch().getHash();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    List<Object[]> result = sql("CREATE BRANCH tempBranch IN nessie");
    assertEquals("created branch", ImmutableList.of(row("Branch", "tempBranch", hash)), result);

    result = sql("CREATE TAG tempTag IN nessie");
    assertEquals("created branch", ImmutableList.of(row("Tag", "tempTag", hash)), result);

    result = sql("CREATE BRANCH tempBranch1 IN nessie AS main");
    assertEquals("created branch", ImmutableList.of(row("Branch", "tempBranch1", hash)), result);

    result = sql("CREATE TAG tempTag1 IN nessie AS main");
    assertEquals("created branch", ImmutableList.of(row("Tag", "tempTag1", hash)), result);

    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("nessie");
    result = sql("CREATE BRANCH tempBranch2");
    assertEquals("created branch", ImmutableList.of(row("Branch", "tempBranch2", hash)), result);

    result = sql("CREATE TAG tempTag2");
    assertEquals("created branch", ImmutableList.of(row("Tag", "tempTag2", hash)), result);
    spark.sessionState().catalogManager().setCurrentCatalog(catalog);

  }

  @After
  public void removeBranches() {
    String hash;
    try {
      hash = client.getTreeApi().getDefaultBranch().getHash();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    client.getTreeApi().getAllReferences().stream().filter(r -> !r.getName().equals("main")).forEach(r -> {
      try {
        if (r instanceof Tag) {
          client.getTreeApi().deleteTag(r.getName(), hash);
        } else {
          client.getTreeApi().deleteBranch(r.getName(), hash);
        }
      } catch (NessieNotFoundException | NessieConflictException e) {
        // pass
      }
    });
  }


}
