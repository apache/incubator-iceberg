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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHadoopCommitsAtomicWrite extends TestHadoopCommitsBase {

  public TestHadoopCommitsAtomicWrite() {
    tables.getConf().set("iceberg.engine.hadoop.file.atomic.write", "true");
  }

  @Test
  public void testAtomicWriteThrow() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.rename(any(), any())).thenThrow(new AssertionError("rename should not have been called!"));
    FSDataOutputStream mockFSDataOutputStream = mock(FSDataOutputStream.class);
    // on the first close we throw the IOException, then we stop throwing.
    // the reason is that the write is surrounded in a try-with-resource, so after the first
    // exception is thrown, close() will be called again - if we throw again there, the same exception will be
    // suppressed on itself, and we'll get "self-suppression not permitted"
    doThrow(new IOException("test injected")).doNothing().when(mockFSDataOutputStream).close();
    when(mockFs.create(any(), anyBoolean())).thenReturn(mockFSDataOutputStream);
    testCommitWithFileSystem(mockFs, true);
  }
}
