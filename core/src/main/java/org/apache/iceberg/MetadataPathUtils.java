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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.WRITE_METADATA_USE_RELATIVE_PATH;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_USE_RELATIVE_PATH_DEFAULT;

/**
 * Utility class that contains path conversion methods.
 */
public final class MetadataPathUtils {

  private MetadataPathUtils() {
  }

  /**
   * Convert a given relative path to absolute path for the table by appending the base table location
   * @param path relative path to be converted
   * @param tableLocation base table location
   * @param shouldUseRelativePaths whether relative paths should be used
   * @return the absolute path
   */
  public static String toAbsolutePath(String path, String tableLocation, boolean shouldUseRelativePaths) {
    Preconditions.checkArgument(path != null && path.trim().length() > 0);
    // TODO: Fix this after tests are changed to always pass the table location. Table location cannot be null.
    if (tableLocation == null) {
      return path;
    }
    // convert to absolute path by appending the table location
    return shouldUseRelativePaths && !path.startsWith(tableLocation) ? tableLocation + "/" + path : path;
  }

  /**
   * Convert a given absolute path to relative path with respect to base table location
   * @param path the absolute path
   * @param tableLocation the base table location
   * @return relative path with respect to the base table location
   */
  public static String toRelativePath(String path, String tableLocation, boolean shouldUseRelativePaths) {
    Preconditions.checkArgument(path != null && path.trim().length() > 0);
    // TODO: Fix this after tests are changed to always pass the table location. Table location cannot be null.
    if (tableLocation == null) {
      return path;
    }
    // convert to relative path by removing the table location
    return shouldUseRelativePaths && path.startsWith(tableLocation) ?
        path.substring(tableLocation.length() + 1) : path;
  }

  /**
   * Return true if relative paths is enabled on this table. Return false otherwise
   * @param properties table properties
   * @return true if "write.metadata.use.relative-path" property is true, false otherwise
   */
  public static boolean shouldUseRelativePath(Map<String, String> properties) {
    return PropertyUtil.propertyAsBoolean(properties, WRITE_METADATA_USE_RELATIVE_PATH,
        WRITE_METADATA_USE_RELATIVE_PATH_DEFAULT);
  }
}
