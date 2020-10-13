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

import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestCleanup extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestManifestCleanup(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testDelete() {
    Assert.assertEquals("Table should start with no manifests",
        0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertEquals("Table should have one append manifest",
        1, table.currentSnapshot().allManifests().size());

    table.newDelete()
        .deleteFromRowFilter(Expressions.alwaysTrue())
        .commit();

    Assert.assertEquals("Table should have one delete manifest",
        1, table.currentSnapshot().allManifests().size());

    table.newAppend().commit();

    Assert.assertEquals("Table should have no manifests",
        0, table.currentSnapshot().allManifests().size());
  }

  @Test
  public void testPartialDelete() {
    Assert.assertEquals("Table should start with no manifests",
        0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot s1 = table.currentSnapshot();
    Assert.assertEquals("Table should have one append manifest",
        1, s1.allManifests().size());

    table.newDelete()
        .deleteFile(FILE_B)
        .commit();

    Snapshot s2 = table.currentSnapshot();
    Assert.assertEquals("Table should have one mixed manifest",
        1, s2.allManifests().size());

    table.newAppend().commit();

    Snapshot s3 = table.currentSnapshot();
    Assert.assertEquals("Table should have the same manifests",
        s2.allManifests(), s3.allManifests());
  }

  @Test
  public void testOverwrite() {
    Assert.assertEquals("Table should start with no manifests",
        0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertEquals("Table should have one append manifest",
        1, table.currentSnapshot().allManifests().size());

    table.newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(FILE_C)
        .addFile(FILE_D)
        .commit();

    Assert.assertEquals("Table should have one delete manifest and one append manifest",
        2, table.currentSnapshot().allManifests().size());

    table.newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(FILE_A)
        .addFile(FILE_B)
        .commit();

    Assert.assertEquals("Table should have one delete manifest and one append manifest",
        2, table.currentSnapshot().allManifests().size());
  }
}
