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

import org.apache.iceberg.types.Comparators;

/**
 * Enum of supported file formats.
 */
public enum FileFormat {
  ORC("orc", true, true),
  PARQUET("parquet", true, true),
  AVRO("avro", true, false),
  METADATA("metadata.json", false, false);

  private final String ext;
  private final boolean splittable;
  private final boolean nullCounts;

  FileFormat(String ext, boolean splittable, boolean nullCounts) {
    this.ext = "." + ext;
    this.splittable = splittable;
    this.nullCounts = nullCounts;
  }

  public boolean isSplittable() {
    return splittable;
  }

  public boolean hasNullCounts() {
    return nullCounts;
  }

  /**
   * Returns filename with this format's extension added, if necessary.
   *
   * @param filename a filename or path
   * @return if the ext is present, the filename, otherwise the filename with ext added
   */
  public String addExtension(String filename) {
    if (filename.endsWith(ext)) {
      return filename;
    }
    return filename + ext;
  }

  public static FileFormat fromFileName(CharSequence filename) {
    for (FileFormat format : FileFormat.values()) {
      int extStart = filename.length() - format.ext.length();
      if (Comparators.charSequences().compare(format.ext, filename.subSequence(extStart, filename.length())) == 0) {
        return format;
      }
    }

    return null;
  }
}
