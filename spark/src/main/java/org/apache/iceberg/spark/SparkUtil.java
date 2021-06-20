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

package org.apache.iceberg.spark;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.util.SerializableConfiguration;

public class SparkUtil {
  private static final Set<String> LOCALITY_WHITELIST_FS = ImmutableSet.of("hdfs");

  private SparkUtil() {
  }

  public static FileIO serializableFileIO(Table table) {
    if (table.io() instanceof HadoopFileIO) {
      // we need to use Spark's SerializableConfiguration to avoid issues with Kryo serialization
      SerializableConfiguration conf = new SerializableConfiguration(((HadoopFileIO) table.io()).conf());
      return new HadoopFileIO(conf::value);
    } else {
      return table.io();
    }
  }

  /**
   * Check whether the partition transforms in a spec can be used to write data.
   *
   * @param spec a PartitionSpec
   * @throws UnsupportedOperationException if the spec contains unknown partition transforms
   */
  public static void validatePartitionTransforms(PartitionSpec spec) {
    if (spec.fields().stream().anyMatch(field -> field.transform() instanceof UnknownTransform)) {
      String unsupported = spec.fields().stream()
          .map(PartitionField::transform)
          .filter(transform -> transform instanceof UnknownTransform)
          .map(Transform::toString)
          .collect(Collectors.joining(", "));

      throw new UnsupportedOperationException(
          String.format("Cannot write using unsupported transforms: %s", unsupported));
    }
  }

  /**
   * A modified version of Spark's LookupCatalog.CatalogAndIdentifier.unapply
   * Attempts to find the catalog and identifier a multipart identifier represents
   * @param nameParts Multipart identifier representing a table
   * @return The CatalogPlugin and Identifier for the table
   */
  public static <C, T> Pair<C, T> catalogAndIdentifier(List<String> nameParts,
                                                       Function<String, C> catalogProvider,
                                                       BiFunction<String[], String, T> identiferProvider,
                                                       C currentCatalog,
                                                       String[] currentNamespace) {
    Preconditions.checkArgument(!nameParts.isEmpty(),
        "Cannot determine catalog and identifier from empty name");

    int lastElementIndex = nameParts.size() - 1;
    String name = nameParts.get(lastElementIndex);

    if (nameParts.size() == 1) {
      // Only a single element, use current catalog and namespace
      return Pair.of(currentCatalog, identiferProvider.apply(currentNamespace, name));
    } else {
      C catalog = catalogProvider.apply(nameParts.get(0));
      if (catalog == null) {
        // The first element was not a valid catalog, treat it like part of the namespace
        String[] namespace =  nameParts.subList(0, lastElementIndex).toArray(new String[0]);
        return Pair.of(currentCatalog, identiferProvider.apply(namespace, name));
      } else {
        // Assume the first element is a valid catalog
        String[] namespace = nameParts.subList(1, lastElementIndex).toArray(new String[0]);
        return Pair.of(catalog, identiferProvider.apply(namespace, name));
      }
    }
  }

  public static boolean isLocalityEnabledDefault(Map<String, String> tableProperties, String fsScheme) {
    String tableLocalityProp = PropertyUtil.propertyAsString(tableProperties, TableProperties.LOCALITY_ENABLED,
        TableProperties.LOCALITY_ENABLED_DEFAULT);
    return tableLocalityProp == null ? LOCALITY_WHITELIST_FS.contains(fsScheme) :
        Boolean.parseBoolean(tableLocalityProp);
  }
}
