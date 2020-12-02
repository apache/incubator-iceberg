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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class TestCatalogUtil {

  @Test
  public void loadCustomCatalog() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    Catalog catalog = CatalogUtil.loadCatalog(TestCatalog.class.getName(), name, options, hadoopConf);
    Assert.assertTrue(catalog instanceof TestCatalog);
    Assert.assertEquals(name, ((TestCatalog) catalog).catalogName);
    Assert.assertEquals(options, ((TestCatalog) catalog).flinkOptions);
  }

  @Test
  public void loadCustomCatalog_withHadoopConfig() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("key", "val");
    String name = "custom";
    Catalog catalog = CatalogUtil.loadCatalog(TestCatalogConfigurable.class.getName(), name, options, hadoopConf);
    Assert.assertTrue(catalog instanceof TestCatalogConfigurable);
    Assert.assertEquals(name, ((TestCatalogConfigurable) catalog).catalogName);
    Assert.assertEquals(options, ((TestCatalogConfigurable) catalog).flinkOptions);
    Assert.assertEquals(hadoopConf, ((TestCatalogConfigurable) catalog).configuration);
  }

  @Test
  public void loadCustomCatalog_NoArgConstructorNotFound() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    AssertHelpers.assertThrows("must have no-arg constructor",
        IllegalArgumentException.class,
        "missing no-arg constructor",
        () -> CatalogUtil.loadCatalog(TestCatalogBadConstructor.class.getName(), name, options, hadoopConf));
  }

  @Test
  public void loadCustomCatalog_NotImplementCatalog() {
    Map<String, String> options = new HashMap<>();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";

    AssertHelpers.assertThrows("must implement catalog",
        IllegalArgumentException.class,
        "does not implement Catalog",
        () -> CatalogUtil.loadCatalog(TestCatalogNoInterface.class.getName(), name, options, hadoopConf));
  }


  @Test
  public void loadCustomFileIO_noArg() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    FileIO fileIO = CatalogUtil.loadFileIO(TestFileIONoArg.class.getName(), properties, null);
    Assert.assertTrue(fileIO instanceof TestFileIONoArg);
    Assert.assertEquals(properties, ((TestFileIONoArg) fileIO).map);
  }

  @Test
  public void loadCustomFileIO_configurable() {
    Configuration configuration = new Configuration();
    configuration.set("key", "val");
    FileIO fileIO = CatalogUtil.loadFileIO(TestFileIOConfigurable.class.getName(), Maps.newHashMap(), configuration);
    Assert.assertTrue(fileIO instanceof TestFileIOConfigurable);
    Assert.assertEquals(configuration, ((TestFileIOConfigurable) fileIO).configuration);
  }

  @Test
  public void loadCustomFileIO_badArg() {
    AssertHelpers.assertThrows("cannot find constructor",
        IllegalArgumentException.class,
        "missing no-arg constructor",
        () -> CatalogUtil.loadFileIO(TestFileIOBadArg.class.getName(), Maps.newHashMap(), null));
  }

  @Test
  public void loadCustomFileIO_badClass() {
    AssertHelpers.assertThrows("cannot cast",
        IllegalArgumentException.class,
        "does not implement FileIO",
        () -> CatalogUtil.loadFileIO(TestFileIONotImpl.class.getName(), Maps.newHashMap(), null));
  }

  public static class TestCatalog extends BaseMetastoreCatalog {

    private String catalogName;
    private Map<String, String> flinkOptions;

    public TestCatalog() {
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.catalogName = name;
      this.flinkOptions = properties;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {

    }
  }

  public static class TestCatalogConfigurable extends BaseMetastoreCatalog implements Configurable {

    private String catalogName;
    private Map<String, String> flinkOptions;
    private Configuration configuration;

    public TestCatalogConfigurable() {
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.catalogName = name;
      this.flinkOptions = properties;
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {

    }
  }

  public static class TestCatalogBadConstructor extends BaseMetastoreCatalog {

    public TestCatalogBadConstructor(String arg) {
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {

    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
    }
  }

  public static class TestCatalogNoInterface {
    public TestCatalogNoInterface() {
    }
  }

  public static class TestFileIOConfigurable implements FileIO, Configurable {

    private Configuration configuration;

    public TestFileIOConfigurable() {
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {

    }

    @Override
    public void deleteDirectory(String directory) {

    }

    public Configuration getConfiguration() {
      return configuration;
    }
  }

  public static class TestFileIONoArg implements FileIO {

    private Map<String, String> map;

    public TestFileIONoArg() {
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {

    }

    @Override
    public void deleteDirectory(String directory) {

    }

    public Map<String, String> getMap() {
      return map;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      map = properties;
    }
  }

  public static class TestFileIOBadArg implements FileIO {

    private final String arg;

    public TestFileIOBadArg(String arg) {
      this.arg = arg;
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {

    }

    @Override
    public void deleteDirectory(String directory) {

    }

    public String getArg() {
      return arg;
    }
  }

  public static class TestFileIONotImpl {
    public TestFileIONotImpl() {
    }
  }
}
