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

package org.apache.iceberg.nessie;

import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.Hash;
import com.dremio.nessie.model.Reference;

class RefreshableReference {

  private Reference reference;
  private final TreeApi client;

  RefreshableReference(Reference reference, TreeApi client) {
    this.reference = reference;
    this.client = client;
  }

  public boolean refresh() throws NessieNotFoundException {
    if (reference instanceof Hash) {
      return false;
    }
    Reference oldReference = reference;
    reference = client.getReferenceByName(reference.getName());
    return !oldReference.equals(reference);
  }

  public boolean isBranch() {
    return reference instanceof Branch;
  }

  public RefreshableReference copy() {
    return new RefreshableReference(reference, client);
  }

  public String getHash() {
    return reference.getHash();
  }

  public Branch getAsBranch() {
    if (!isBranch()) {
      throw new IllegalArgumentException("Reference is not a branch");
    }
    return (Branch) reference;
  }

  public void checkMutable() {
    if (!isBranch()) {
      throw new IllegalArgumentException("You can only mutate tables when using a branch.");
    }
  }

  public String getName() {
    return reference.getName();
  }
}
