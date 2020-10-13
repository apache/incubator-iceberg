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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshot extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestSnapshot(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testAppendFilesFromTable() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    // collect data files from deserialization
    Iterable<DataFile> filesToAdd = table.currentSnapshot().addedFiles();

    table.newDelete().deleteFile(FILE_A).deleteFile(FILE_B).commit();

    Snapshot oldSnapshot = table.currentSnapshot();

    AppendFiles fastAppend = table.newFastAppend();
    for (DataFile file : filesToAdd) {
      fastAppend.appendFile(file);
    }

    Snapshot newSnapshot = fastAppend.apply();
    validateSnapshot(oldSnapshot, newSnapshot, FILE_A, FILE_B);
  }

  @Test
  public void testAppendFoundFiles() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Iterable<DataFile> filesToAdd = FindFiles.in(table)
        .inPartition(table.spec(), StaticDataTask.Row.of(0))
        .inPartition(table.spec(), StaticDataTask.Row.of(1))
        .collect();

    table.newDelete().deleteFile(FILE_A).deleteFile(FILE_B).commit();

    Snapshot oldSnapshot = table.currentSnapshot();

    AppendFiles fastAppend = table.newFastAppend();
    for (DataFile file : filesToAdd) {
      fastAppend.appendFile(file);
    }

    Snapshot newSnapshot = fastAppend.apply();
    validateSnapshot(oldSnapshot, newSnapshot, FILE_A, FILE_B);
  }

}
