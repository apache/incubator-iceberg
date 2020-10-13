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

package org.apache.iceberg.hive;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;

public class TestHiveMetastore {

  // create the metastore handlers based on whether we're working with Hive2 or Hive3 dependencies
  // we need to do this because there is a breaking API change between Hive2 and Hive3
  private static final DynConstructors.Ctor<HiveMetaStore.HMSHandler> HMS_HANDLER_CTOR = DynConstructors.builder()
          .impl(HiveMetaStore.HMSHandler.class, String.class, Configuration.class)
          .impl(HiveMetaStore.HMSHandler.class, String.class, HiveConf.class)
          .build();

  private static final DynMethods.StaticMethod GET_BASE_HMS_HANDLER = DynMethods.builder("getProxy")
          .impl(RetryingHMSHandler.class, Configuration.class, IHMSHandler.class, boolean.class)
          .impl(RetryingHMSHandler.class, HiveConf.class, IHMSHandler.class, boolean.class)
          .buildStatic();

  private File hiveLocalDir;
  private HiveConf hiveConf;
  private ExecutorService executorService;
  private TServer server;
  private HiveMetaStore.HMSHandler baseHandler;

  public void start() {
    try {
      hiveLocalDir = createTempDirectory("hive", asFileAttribute(fromString("rwxrwxrwx"))).toFile();
      File derbyLogFile = new File(hiveLocalDir, "derby.log");
      System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
      setupMetastoreDB("jdbc:derby:" + getDerbyPath() + ";create=true");

      TServerSocket socket = new TServerSocket(0);
      int port = socket.getServerSocket().getLocalPort();
      hiveConf = newHiveConf(port);
      server = newThriftServer(socket, hiveConf);
      executorService = Executors.newSingleThreadExecutor();
      executorService.submit(() -> server.serve());
    } catch (Exception e) {
      throw new RuntimeException("Cannot start TestHiveMetastore", e);
    }
  }

  public void stop() {
    if (server != null) {
      server.stop();
    }
    if (executorService != null) {
      executorService.shutdown();
    }
    if (hiveLocalDir != null) {
      hiveLocalDir.delete();
    }
    if (baseHandler != null) {
      baseHandler.shutdown();
    }
  }

  public HiveConf hiveConf() {
    return hiveConf;
  }

  public String getDatabasePath(String dbName) {
    File dbDir = new File(hiveLocalDir, dbName + ".db");
    return dbDir.getPath();
  }

  private TServer newThriftServer(TServerSocket socket, HiveConf conf) throws Exception {
    HiveConf serverConf = new HiveConf(conf);
    serverConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:" + getDerbyPath() + ";create=true");
    baseHandler = HMS_HANDLER_CTOR.newInstance("new db based metaserver", serverConf);
    IHMSHandler handler = GET_BASE_HMS_HANDLER.invoke(serverConf, baseHandler, false);

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
        .processor(new TSetIpAddressProcessor<>(handler))
        .transportFactory(new TTransportFactory())
        .protocolFactory(new TBinaryProtocol.Factory())
        .minWorkerThreads(3)
        .maxWorkerThreads(5);

    return new TThreadPoolServer(args);
  }

  private HiveConf newHiveConf(int port) {
    HiveConf newHiveConf = new HiveConf(new Configuration(), TestHiveMetastore.class);
    newHiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
    newHiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + hiveLocalDir.getAbsolutePath());
    newHiveConf.set(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname, "false");
    newHiveConf.set(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname, "false");
    newHiveConf.set("iceberg.hive.client-pool-size", "2");
    return newHiveConf;
  }

  private void setupMetastoreDB(String dbURL) throws SQLException, IOException {
    Connection connection = DriverManager.getConnection(dbURL);
    ScriptRunner scriptRunner = new ScriptRunner(connection, true, true);

    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("hive-schema-3.1.0.derby.sql");
    try (Reader reader = new InputStreamReader(inputStream)) {
      scriptRunner.runScript(reader);
    }
  }

  private String getDerbyPath() {
    File metastoreDB = new File(hiveLocalDir, "metastore_db");
    return metastoreDB.getPath();
  }
}
