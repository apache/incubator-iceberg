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

package org.apache.iceberg.actions;

import org.apache.iceberg.Table;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.spark.actions.BaseExpireSnapshotsSparkAction;
import org.apache.iceberg.spark.actions.BaseRemoveOrphanFilesSparkAction;
import org.apache.iceberg.spark.actions.BaseRewriteDeletesSparkAction;
import org.apache.iceberg.spark.actions.BaseRewriteManifestsSparkAction;
import org.apache.spark.sql.SparkSession;

public class Actions {

  /*
  We load the actual implementation of Actions via reflection to allow for differences
  between the major Spark APIs while still defining the API in this class.
  */
  private static final String IMPL_NAME = "SparkActions";
  private static DynConstructors.Ctor<Actions> implConstructor;

  private static String implClass() {
    return Actions.class.getPackage().getName() + "." + IMPL_NAME;
  }

  private static DynConstructors.Ctor<Actions> actionConstructor() {
    if (implConstructor == null) {
      String className = implClass();
      try {
        implConstructor =
            DynConstructors.builder().hiddenImpl(className, SparkSession.class, Table.class).buildChecked();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Cannot find appropriate Actions implementation on the classpath.", e);
      }
    }
    return implConstructor;
  }

  private SparkSession spark;
  private Table table;

  protected Actions(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
  }

  public static Actions forTable(SparkSession spark, Table table) {
    return actionConstructor().newInstance(spark, table);
  }

  public static Actions forTable(Table table) {
    return forTable(SparkSession.active(), table);
  }

  public RemoveOrphanFilesAction removeOrphanFiles() {
    BaseRemoveOrphanFilesSparkAction delegate = new BaseRemoveOrphanFilesSparkAction(spark, table);
    return new RemoveOrphanFilesAction(delegate);
  }

  public RewriteManifestsAction rewriteManifests() {
    BaseRewriteManifestsSparkAction delegate = new BaseRewriteManifestsSparkAction(spark, table);
    return new RewriteManifestsAction(delegate);
  }

  public RewriteDataFilesAction rewriteDataFiles() {
    return new RewriteDataFilesAction(spark, table);
  }

  public RewriteDeletes rewriteDeletes() {
    return new BaseRewriteDeletesSparkAction(spark, table);
  }

  public ExpireSnapshotsAction expireSnapshots() {
    BaseExpireSnapshotsSparkAction delegate = new BaseExpireSnapshotsSparkAction(spark, table);
    return new ExpireSnapshotsAction(delegate);
  }

  /**
   * Converts the provided table into an Iceberg table in place. The table will no longer be accessible by it's
   * previous implementation
   *
   * @param tableName Table to be converted
   * @return {@link CreateAction} to perform migration
   */
  public static CreateAction migrate(String tableName) {
    try {
      return DynMethods.builder("migrate")
          .impl(implClass(), String.class).buildStaticChecked()
          .invoke(tableName);
    } catch (NoSuchMethodException ex) {
      throw new UnsupportedOperationException("Migrate is not implemented for this version of Spark");
    }
  }

  /**
   * Converts the provided table into an Iceberg table in place. The table will no longer be accessible by it's
   * previous implementation
   *
   * @param tableName Table to be converted
   * @param spark     Spark session to use for looking up table
   * @return {@link CreateAction} to perform migration
   */
  public static CreateAction migrate(SparkSession spark, String tableName) {
    try {
      return DynMethods.builder("migrate")
          .impl(implClass(), SparkSession.class, String.class).buildStaticChecked()
          .invoke(spark, tableName);
    } catch (NoSuchMethodException ex) {
      throw new UnsupportedOperationException("Migrate is not implemented for this version of Spark");
    }
  }

  /**
   * Creates an independent Iceberg table based on a given table. The new Iceberg table can be altered, appended or
   * deleted without causing any change to the original. New data and metadata will be created in the default
   * location for tables of this name in the destination catalog.
   *
   * @param sourceTable Original table which is the basis for the new Iceberg table
   * @param destTable   New Iceberg table being created
   * @return {@link SnapshotAction} to perform snapshot
   */
  public static SnapshotAction snapshot(SparkSession spark, String sourceTable, String destTable) {
    try {
      return DynMethods.builder("snapshot")
          .impl(implClass(), SparkSession.class, String.class, String.class).buildStaticChecked()
          .invoke(spark, sourceTable, destTable);
    } catch (NoSuchMethodException ex) {
      throw new UnsupportedOperationException("Snapshot is not implemented for this version of Spark");
    }
  }

  /**
   * Creates an independent Iceberg table based on a given table. The new Iceberg table can be altered, appended or
   * deleted without causing any change to the original. New data and metadata will be created in the default
   * location for tables of this name in the destination catalog.
   *
   * @param sourceTable Original table which is the basis for the new Iceberg table
   * @param destTable   New Iceberg table being created
   * @return {@link SnapshotAction} to perform snapshot
   */
  public static SnapshotAction snapshot(String sourceTable, String destTable) {
    try {
      return DynMethods.builder("snapshot")
          .impl(implClass(), String.class, String.class).buildStaticChecked()
          .invoke(sourceTable, destTable);
    } catch (NoSuchMethodException ex) {
      throw new UnsupportedOperationException("Snapshot is not implemented for this version of Spark");
    }
  }

  protected SparkSession spark() {
    return spark;
  }

  protected Table table() {
    return table;
  }

}
