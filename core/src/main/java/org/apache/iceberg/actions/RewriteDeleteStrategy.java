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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;

public interface RewriteDeleteStrategy {

  /**
   * Returns the name of this rewrite deletes strategy
   */
  String name();

  /**
   * Returns the table being modified by this rewrite strategy
   */
  Table table();

  /**
   * Select the deletes to rewrite.
   *
   * @return iterable of original delete file to be replaced.
   */
  Iterable<DeleteFile> selectDeletes();

  /**
   * Define how to rewrite the deletes.
   *
   * @return iterable of delete files used to replace the original delete files.
   */
  Iterable<DeleteFile> rewriteDeletes();

  /**
   * Sets options to be used with this strategy
   */
  RewriteDeleteStrategy options(Map<String, String> options);

  /**
   * Returns a set of options which this rewrite strategy can use. This is an allowed-list and any options not
   * specified here will be rejected at runtime.
   */
  Set<String> validOptions();
}
