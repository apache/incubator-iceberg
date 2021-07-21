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

package org.apache.iceberg.aliyun;

import com.aliyun.oss.OSS;
import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;

/**
 * Interface to customize OSS clients used by Iceberg.
 * A custom factory must have a no-arg constructor, and use {@link #initialize(Map)} to initialize the factory.
 */
public interface AliyunClientFactory extends Serializable {
  /**
   * Create an aliyun OSS client.
   *
   * @return oss client.
   */
  OSS oss();

  /**
   * Initialize Aliyun client factory from catalog properties.
   *
   * @param properties catalog properties
   */
  void initialize(Map<String, String> properties);

  /**
   * Returns an initialized {@link AliyunProperties}
   */
  AliyunProperties aliyunProperties();

  static AliyunClientFactory load(Map<String, String> properties) {
    String impl = properties.getOrDefault("client.factory", DefaultAliyunClientFactory.class.getName());
    return load(impl, properties);
  }

  /**
   * Load an implemented {@link AliyunClientFactory} based on the class name, and initialize it.
   *
   * @param impl       the class name.
   * @param properties to initialize the factory.
   * @return an initialized {@link AliyunClientFactory}.
   */
  static AliyunClientFactory load(String impl, Map<String, String> properties) {
    DynConstructors.Ctor<AliyunClientFactory> ctor;
    try {
      ctor = DynConstructors.builder(AliyunClientFactory.class).impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot initialize AliyunClientFactory, missing no-arg constructor: %s", impl), e);
    }

    AliyunClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize AliyunClientFactory, %s does not implement AliyunClientFactory.", impl), e);
    }

    factory.initialize(properties);
    return factory;
  }
}
