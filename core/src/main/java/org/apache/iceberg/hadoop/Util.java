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

package org.apache.iceberg.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  private Util() {
  }

  public static FileSystem getFs(Path path, Configuration conf) {
    try {
      return path.getFileSystem(conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", path);
    }
  }

  public static boolean shouldUseAtomicWrite(Path path, Configuration conf) {
    String scheme = path.toUri().getScheme();
    String fullConfigName = String.join(".",
            ConfigProperties.USE_ATOMIC_WRITE_PREFIX,
            scheme,
            ConfigProperties.USE_ATOMIC_WRITE_SUFFIX);
    return conf.getBoolean(fullConfigName, false);
  }

  public static String[] blockLocations(CombinedScanTask task, Configuration conf) {
    Set<String> locationSets = Sets.newHashSet();
    for (FileScanTask f : task.files()) {
      Path path = new Path(f.file().path().toString());
      try {
        FileSystem fs = path.getFileSystem(conf);
        for (BlockLocation b : fs.getFileBlockLocations(path, f.start(), f.length())) {
          locationSets.addAll(Arrays.asList(b.getHosts()));
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to get block locations for path {}", path, ioe);
      }
    }

    return locationSets.toArray(new String[0]);
  }

  public static String[] blockLocations(FileIO io, CombinedScanTask task) {
    Set<String> locations = Sets.newHashSet();
    for (FileScanTask f : task.files()) {
      InputFile in = io.newInputFile(f.file().path().toString());
      if (in instanceof HadoopInputFile) {
        Collections.addAll(locations, ((HadoopInputFile) in).getBlockLocations(f.start(), f.length()));
      }
    }

    return locations.toArray(HadoopInputFile.NO_LOCATION_PREFERENCE);
  }

  /**
   * From Apache Spark
   *
   * Convert URI to String.
   * Since URI.toString does not decode the uri, e.g. change '%25' to '%'.
   * Here we create a hadoop Path with the given URI, and rely on Path.toString
   * to decode the uri
   * @param uri the URI of the path
   * @return the String of the path
   */
  public static String uriToString(URI uri) {
    return new Path(uri).toString();
  }
}
