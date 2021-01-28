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

package org.apache.iceberg.hadoop;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHadoopCommitsAtomicRename extends TestHadoopCommitsBase {

  @Test
  public void testRenameReturnFalse() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.rename(any(), any())).thenReturn(false);
    testCommitWithFileSystem(mockFs, false);
  }

  @Test
  public void testRenameThrow() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.rename(any(), any())).thenThrow(new IOException("test injected"));
    testCommitWithFileSystem(mockFs, false);
  }
}
