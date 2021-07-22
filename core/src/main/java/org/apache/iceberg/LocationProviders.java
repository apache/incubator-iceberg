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
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.OBJECT_STORE_PATH;

public class LocationProviders {

  private LocationProviders() {
  }

  public static LocationProvider locationsFor(String location, Map<String, String> properties) {
    if (properties.containsKey(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)) {
      String impl = properties.get(TableProperties.WRITE_LOCATION_PROVIDER_IMPL);
      DynConstructors.Ctor<LocationProvider> ctor;
      try {
        ctor = DynConstructors.builder(LocationProvider.class)
            .impl(impl, String.class, Map.class)
            .impl(impl).buildChecked(); // fall back to no-arg constructor
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(String.format(
            "Unable to find a constructor for implementation %s of %s. " +
                "Make sure the implementation is in classpath, and that it either " +
                "has a public no-arg constructor or a two-arg constructor " +
                "taking in the string base table location and its property string map.",
            impl, LocationProvider.class), e);
      }
      try {
        return ctor.newInstance(location, properties);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format("Provided implementation for dynamic instantiation should implement %s.",
                LocationProvider.class), e);
      }
    } else if (PropertyUtil.propertyAsBoolean(properties,
        TableProperties.OBJECT_STORE_ENABLED,
        TableProperties.OBJECT_STORE_ENABLED_DEFAULT)) {
      return new ObjectStoreLocationProvider(location, properties);
    } else {
      return new DefaultLocationProvider(location, properties);
    }
  }

  private static String defaultDataLocation(String tableLocation, Map<String, String> properties) {
    return properties.getOrDefault(TableProperties.WRITE_NEW_DATA_LOCATION, String.format("%s/data", tableLocation));
  }

  static class DefaultLocationProvider implements LocationProvider {
    private final String dataLocation;

    DefaultLocationProvider(String tableLocation, Map<String, String> properties) {
      this.dataLocation = stripTrailingSlash(defaultDataLocation(tableLocation, properties));
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return String.format("%s/%s/%s", dataLocation, spec.partitionToPath(partitionData), filename);
    }

    @Override
    public String newDataLocation(String filename) {
      return String.format("%s/%s", dataLocation, filename);
    }
  }

  static class ObjectStoreLocationProvider implements LocationProvider {
    private static final Transform<String, Integer> HASH_FUNC = Transforms
        .bucket(Types.StringType.get(), Integer.MAX_VALUE);

    private final String storageLocation;
    private final String context;

    ObjectStoreLocationProvider(String tableLocation, Map<String, String> properties) {
      this.storageLocation = stripTrailingSlash(properties.getOrDefault(OBJECT_STORE_PATH,
          defaultDataLocation(tableLocation, properties)));
      this.context = pathContext(tableLocation);
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      return newDataLocation(String.format("%s/%s", spec.partitionToPath(partitionData), filename));
    }

    @Override
    public String newDataLocation(String filename) {
      int hash = HASH_FUNC.apply(filename);
      return String.format("%s/%08x/%s/%s", storageLocation, hash, context, filename);
    }

    private static String pathContext(String tableLocation) {
      Path dataPath = new Path(tableLocation);
      Path parent = dataPath.getParent();
      String resolvedContext;
      if (parent != null) {
        // remove the data folder
        resolvedContext = String.format("%s/%s", parent.getName(), dataPath.getName());
      } else {
        resolvedContext = dataPath.getName();
      }

      Preconditions.checkState(
          !resolvedContext.endsWith("/"),
          "Path context must not end with a slash.");

      return resolvedContext;
    }
  }

  private static String stripTrailingSlash(String path) {
    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }
}
