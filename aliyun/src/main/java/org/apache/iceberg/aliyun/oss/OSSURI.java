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

package org.apache.iceberg.aliyun.oss;

import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public class OSSURI {
  private static final String SCHEMA_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";
  private static final Set<String> VALID_SCHEMAS = ImmutableSet.of("https", "oss");

  private final String location;
  private final String bucket;
  private final String key;

  /**
   * Creates a new OSSURI based on the bucket and key parsed from the location as defined in:
   * https://www.alibabacloud.com/help/doc-detail/31827.htm
   * <p>
   * Supported access styles are Virtual Hosted addresses and oss://... URIs.
   *
   * @param location fully qualified URI.
   */
  public OSSURI(String location) {
    Preconditions.checkNotNull(location, "OSS location cannot be null.");

    this.location = location;
    String[] schemaSplit = location.split(SCHEMA_DELIM, -1);
    ValidationException.check(schemaSplit.length == 2, "Invalid OSS URI: %s", location);

    String schema = schemaSplit[0];
    ValidationException.check(VALID_SCHEMAS.contains(schema.toLowerCase()), "Invalid schema: %s", schema);

    String[] authoritySplit = schemaSplit[1].split(PATH_DELIM, 2);
    ValidationException.check(authoritySplit.length == 2, "Invalid OSS URI: %s", location);
    ValidationException.check(!authoritySplit[1].trim().isEmpty(), "Invalid OSS key: %s", location);
    this.bucket = authoritySplit[0];

    // Strip query and fragment if they exist
    String path = authoritySplit[1];
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.key = path;
  }

  /**
   * Return OSS bucket name.
   */
  public String bucket() {
    return bucket;
  }

  /**
   * Return OSS object key name.
   */
  public String key() {
    return key;
  }

  /**
   * Returns original, unmodified OSS URI location.
   */
  public String location() {
    return location;
  }

  @Override
  public String toString() {
    return location;
  }
}
