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
 * Catalog methods for working with branches and tags.
 */
public interface SupportsBranches {

  /**
   * Create a {@link Reference.Branch} named 'name' at the default base reference.
   * @param name name of branch to create.
   */
  default void createBranch(String name) {
    createBranch(name, null);
  }

  /**
   * Create a {@link Reference.Branch} named 'name' at the given base reference.
   * @param name name of branch to create.
   */
  default void createBranch(String name, String base) {
    createReference(Reference.Branch.of(name, base));
  }

  /**
   * Create a {@link Reference.Tag} named 'name' at the default base reference.
   * @param name name of tag to create.
   */
  default void createTag(String name) {
    createTag(name, null);
  }

  /**
   * Create a {@link Reference.Tag} named 'name' at the given base reference.
   * @param name name of tag to create.
   */
  default void createTag(String name, String base) {
    createReference(Reference.Tag.of(name, base));
  }

  /**
   * Create a reference as defined in ref.
   * @param ref name of tag to create
   * @throws org.apache.iceberg.exceptions.AlreadyExistsException if the reference already exists.
   * @throws IllegalArgumentException if the hash to create at does not exist.
   */
  void createReference(Reference ref);

  /**
   * List all branches and tags known to this catalog.
   */
  Iterable<Reference> listReferences();

  /**
   * Delete a reference without a safety check. This force deletes the reference.
   * @param name name of reference to delete.
   * @throws IllegalArgumentException if the reference does not exist or if the reference name is a Hash.
   */
  void deleteReference(String name);

  /**
   * Delete a reference with a safety check. The hash of ref must be the current hash of ref on the server or the
   * operation fails.
   *
   * @throws IllegalArgumentException if the reference does not exist or if the reference name is a Hash.
   * @throws IllegalStateException if the reference hash is not up to date.
   */
  void deleteReference(Reference ref);

  /**
   * Sets the current reference of this Catalog.
   *
   * <p>All further operations will take place on this Reference unless specified.
   *
   * @param name name of reference to set.
   * @throws IllegalArgumentException if the reference does not exist.
   */
  void setCurrentReference(String name);

  /**
   * Show the current reference of this Catalog.
   */
  Reference currentReference();

  /**
   * Get an arbitrary reference by name. This could be a Hash, Branch or tag.
   *
   * @param name name of reference to set.
   * @throws IllegalArgumentException if the reference does not exist.
   */
  Reference referenceByName(String name);
}
