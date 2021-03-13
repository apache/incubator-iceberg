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

package org.apache.iceberg.catalog;

/**
 * Reference used for SupportsNamespace.
 *
 * <p>A reference can be one of three things:
 * <ul>
 *   <li>Branch - a pointer to a commit hash that can be modified</li>
 *   <li>Tag - a pointer to a commit hash that can't be modified</li>
 *   <li>Hash - a specific commit hash</li>
 * </ul>
 *
 * <p>A reference is either a Hash or a pointer to a hash. A hash is a specific point in the history of a branching
 * catalog's history. A reference has two components: a name and a hash. The name is the human readable name that points
 * to a specific hash.
 *
 * <p>A Tag cannot be updated or modified. It can be reassigned to another hash only. A hash can't be changed at all. It
 * is an immutable identifier to a specific point in the branching catalog's history. A branch can be modified, any
 * catalog which plans to write or modify iceberg tables should set its reference to a branch.
 */
public interface Reference {

  /**
   * the name of this reference.
   */
  String name();

  /**
   * the hash this reference points to.
   */
  String hash();

  class Branch implements Reference {
    private final String name;
    private final String hash;

    private Branch(String name, String hash) {
      this.name = name;
      this.hash = hash;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String hash() {
      return hash;
    }

    public static Reference of(String name, String base) {
      return new Branch(name, base);
    }
  }

  class Tag implements Reference {
    private final String name;
    private final String hash;

    private Tag(String name, String hash) {
      this.name = name;
      this.hash = hash;
    }

    public static Reference of(String name, String base) {
      return new Tag(name, base);
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String hash() {
      return hash;
    }
  }

  class Hash implements Reference {
    private final String hash;

    private Hash(String hash) {
      this.hash = hash;
    }

    public static Reference of(String hash) {
      return new Hash(hash);
    }

    @Override
    public String name() {
      return hash;
    }

    @Override
    public String hash() {
      return hash;
    }
  }
}
