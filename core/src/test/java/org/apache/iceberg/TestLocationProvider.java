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
import org.apache.iceberg.io.LocationProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestLocationProvider extends TableTestBase {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestLocationProvider(int formatVersion) {
    super(formatVersion);
  }

  // publicly visible for testing to be dynamically loaded
  public static class TwoArgDynamicallyLoadedLocationProvider implements LocationProvider {
    String tableLocation;
    Map<String, String> properties;

    public TwoArgDynamicallyLoadedLocationProvider(String tableLocation, Map<String, String> properties) {
      this.tableLocation = tableLocation;
      this.properties = properties;
    }

    @Override
    public String newDataLocation(String filename) {
      return String.format("%s/test_custom_provider/%s", this.tableLocation, filename);
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      throw new RuntimeException("Test custom provider does not expect any invocation");
    }
  }

  // publicly visible for testing to be dynamically loaded
  public static class NoArgDynamicallyLoadedLocationProvider implements LocationProvider {
    // No-arg public constructor

    @Override
    public String newDataLocation(String filename) {
      return String.format("test_no_arg_provider/%s", filename);
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      throw new RuntimeException("Test custom provider does not expect any invocation");
    }
  }

  // publicly visible for testing to be dynamically loaded
  public static class InvalidArgTypesDynamicallyLoadedLocationProvider implements LocationProvider {

    public InvalidArgTypesDynamicallyLoadedLocationProvider(Integer bogusArg1, String bogusArg2) {
    }

    @Override
    public String newDataLocation(String filename) {
      throw new RuntimeException("Invalid provider should have not been instantiated!");
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename) {
      throw new RuntimeException("Invalid provider should have not been instantiated!");
    }
  }

  // publicly visible for testing to be dynamically loaded
  public static class InvalidNoInterfaceDynamicallyLoadedLocationProvider {
    // Default no-arg constructor is present, but does not impelemnt interface LocationProvider
  }

  @Test
  public void testDefaultLocationProvider() {
    this.table.updateProperties()
        .commit();

    this.table.locationProvider().newDataLocation("my_file");
    Assert.assertEquals(
        "Default data path should have table location as root",
        String.format("%s/data/%s", this.table.location(), "my_file"),
        this.table.locationProvider().newDataLocation("my_file")
    );
  }

  @Test
  public void testDefaultLocationProviderWithCustomDataLocation() {
    this.table.updateProperties()
        .set(TableProperties.WRITE_NEW_DATA_LOCATION, "new_location")
        .commit();

    this.table.locationProvider().newDataLocation("my_file");
    Assert.assertEquals(
        "Default location provider should allow custom path location",
        "new_location/my_file",
        this.table.locationProvider().newDataLocation("my_file")
    );
  }

  @Test
  public void testNoArgDynamicallyLoadedLocationProvider() {
    String invalidImpl = String.format("%s$%s",
        this.getClass().getCanonicalName(),
        NoArgDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table.updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, invalidImpl)
        .commit();

    Assert.assertEquals(
        "Custom provider should take base table location",
        "test_no_arg_provider/my_file",
        this.table.locationProvider().newDataLocation("my_file")
    );
  }

  @Test
  public void testTwoArgDynamicallyLoadedLocationProvider() {
    this.table.updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL,
            String.format("%s$%s",
                this.getClass().getCanonicalName(),
                TwoArgDynamicallyLoadedLocationProvider.class.getSimpleName()))
        .commit();

    Assert.assertTrue(String.format("Table should load impl defined in its properties"),
        this.table.locationProvider() instanceof TwoArgDynamicallyLoadedLocationProvider
    );

    Assert.assertEquals(
        "Custom provider should take base table location",
        String.format("%s/test_custom_provider/%s", this.table.location(), "my_file"),
        this.table.locationProvider().newDataLocation("my_file")
    );
  }

  @Test
  public void testDynamicallyLoadedLocationProviderNotFound() {
    String nonExistentImpl = String.format("%s$NonExistent%s",
        this.getClass().getCanonicalName(),
        TwoArgDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table.updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, nonExistentImpl)
        .commit();

    AssertHelpers.assertThrows("Non-existent implementation should fail on finding constructor",
        IllegalArgumentException.class,
        String.format("Unable to find a constructor for implementation %s of %s. ",
            nonExistentImpl, LocationProvider.class),
        () -> table.locationProvider()
    );
  }

  @Test
  public void testInvalidNoInterfaceDynamicallyLoadedLocationProvider() {
    String invalidImpl = String.format("%s$%s",
        this.getClass().getCanonicalName(),
        InvalidNoInterfaceDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table.updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, invalidImpl)
        .commit();

    AssertHelpers.assertThrows(
        "Class with missing interface implementation should fail on instantiation.",
        IllegalArgumentException.class,
        String.format("Provided implementation for dynamic instantiation should implement %s",
            LocationProvider.class),
        () -> table.locationProvider()
    );
  }

  @Test
  public void testInvalidArgTypesDynamicallyLoadedLocationProvider() {
    String invalidImpl = String.format("%s$%s",
        this.getClass().getCanonicalName(),
        InvalidArgTypesDynamicallyLoadedLocationProvider.class.getSimpleName());
    this.table.updateProperties()
        .set(TableProperties.WRITE_LOCATION_PROVIDER_IMPL, invalidImpl)
        .commit();

    AssertHelpers.assertThrows("Implementation with invalid arg types should fail on finding constructor",
        IllegalArgumentException.class,
        String.format("Unable to find a constructor for implementation %s of %s. ",
            invalidImpl, LocationProvider.class),
        () -> table.locationProvider()
    );
  }

  @Test
  public void testObjectStorageLocationProvider() {
    table.updateProperties()
        .set(TableProperties.OBJECT_STORE_ENABLED, "true")
        .commit();

    Assert.assertTrue("default data location should be used when object storage path not set",
        table.locationProvider().newDataLocation("file").contains(table.location() + "/data"));

    String storagePath = "s3://random/location";
    table.updateProperties()
        .set(TableProperties.OBJECT_STORE_ENABLED, "true")
        .set(TableProperties.OBJECT_STORE_PATH, storagePath)
        .commit();

    Assert.assertTrue("storage path should be used when set",
        table.locationProvider().newDataLocation("file").contains(storagePath));
  }
}
