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

/**
 * Content type stored in a file, one of DATA, POSITION_DELETES, EQUALITY_DELETES or INDEX_DATA.
 */
public enum FileContent {
  DATA(0),
  POSITION_DELETES(1),
  EQUALITY_DELETES(2),
  INDEX_DATA(3);

  private final int id;

  FileContent(int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }
}
