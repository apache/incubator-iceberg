/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell.impl;

import java.util.List;
import org.apache.iceberg.dell.ObjectBaseKey;
import org.apache.iceberg.dell.ObjectKeys;

/**
 * impl of {@link ObjectKeys}
 */
public class ObjectKeysImpl implements ObjectKeys {

  private final ObjectBaseKey baseKey;

  /**
   * lazy results of {@link #getBaseKeyParts()}
   */
  private volatile List<String> lazyBaseKeyParts;

  public ObjectKeysImpl(ObjectBaseKey baseKey) {
    this.baseKey = baseKey;
  }

  @Override
  public ObjectBaseKey getBaseKey() {
    return baseKey;
  }

  /**
   * use field to cache result parts.
   *
   * @return cached base key parts
   */
  @Override
  public List<String> getBaseKeyParts() {
    // code isn't full thread safe. but creating instances in multiple times is fine
    if (lazyBaseKeyParts == null) {
      lazyBaseKeyParts = ObjectKeys.super.getBaseKeyParts();
    }
    return lazyBaseKeyParts;
  }
}
