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

package org.apache.iceberg.dell;

import java.util.Objects;

/**
 * a immutable record class of object key
 */
public class ObjectKey {
  /**
   * bucket
   */
  private final String bucket;

  /**
   * key
   */
  private final String key;

  public ObjectKey(String bucket, String key) {
    if (bucket == null || key == null) {
      throw new IllegalArgumentException(String.format("bucket %s and key %s must be not null", bucket, key));
    }
    this.bucket = bucket;
    this.key = key;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    return "ObjectKey{" +
        "bucket='" + bucket + '\'' +
        ", key='" + key + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ObjectKey objectKey = (ObjectKey) o;
    return Objects.equals(bucket, objectKey.bucket) && Objects.equals(key, objectKey.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, key);
  }
}
