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
 * a immutable record class of object base key which allow not null bucket and key.
 */
public class ObjectBaseKey {

  private final String bucket;
  public final String key;

  public ObjectBaseKey(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;
  }

  public ObjectKey asKey() {
    if (bucket == null) {
      throw new IllegalArgumentException(String.format(
          "fail to cast base key %s as object key, bucket is unknown",
          this));
    }
    return new ObjectKey(bucket, key == null ? "" : key);
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    return "ObjectBaseKey{" +
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
    ObjectBaseKey that = (ObjectBaseKey) o;
    return Objects.equals(bucket, that.bucket) && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, key);
  }
}
