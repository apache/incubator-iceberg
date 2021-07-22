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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.dell.impl.EcsClientImpl;
import org.apache.iceberg.dell.impl.ObjectKeysImpl;
import org.apache.iceberg.util.PropertyUtil;

/**
 * property constants of catalog
 */
public interface EcsCatalogProperties {

  /**
   * static access key id
   */
  String ACCESS_KEY_ID = "s3.access.key.id";

  /**
   * static secret access key
   */
  String SECRET_ACCESS_KEY = "s3.secret.access.key";

  /**
   * s3 endpoint
   */
  String ENDPOINT = "s3.endpoint";

  /**
   * s3 region
   */
  String REGION = "s3.region";

  /**
   * base key which is like "bucket/key"
   * <p>
   * In current version, the bucket of base must be present.
   * <p>
   * In future, we'll support list buckets in catalog
   */
  String BASE_KEY = "s3.base.key";

  /**
   * factory method of {@link EcsClient}.
   * <p>
   * The method should be static and use format like: "org.apache.iceberg.dell.EcsClient#create"
   * <p>
   * The method must have only one parameter. And return exact {@link EcsClient} type.
   */
  String ECS_CLIENT_FACTORY = "ecs.client.factory";

  /**
   * get object base key from properties
   *
   * @param properties is property
   * @return instance of ObjectBaseKey
   */
  static ObjectBaseKey getObjectBaseKey(Map<String, String> properties) {
    String baseKey = properties.get(BASE_KEY);
    if (baseKey == null) {
      throw new IllegalArgumentException(String.format("missing property %s", BASE_KEY));
    }
    String[] baseKeySplits = baseKey.split(ObjectKeys.DELIMITER, 2);
    if (baseKeySplits.length == 1) {
      return new ObjectBaseKey(baseKeySplits[0], null);
    } else {
      return new ObjectBaseKey(baseKeySplits[0], baseKeySplits[1]);
    }
  }

  /**
   * get ecs client from factory
   *
   * @return ecs client that created by factory
   */
  static Optional<EcsClient> getEcsClientFromFactory(Map<String, String> properties) {
    String factory = properties.get(ECS_CLIENT_FACTORY);
    if (factory == null || factory.isEmpty()) {
      return Optional.empty();
    }
    String[] classAndMethod = factory.split("#", 2);
    if (classAndMethod.length != 2) {
      throw new IllegalArgumentException(String.format("invalid property %s", ECS_CLIENT_FACTORY));
    }
    Class<?> clazz;
    try {
      clazz = Class.forName(classAndMethod[0], true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(String.format("invalid property %s", ECS_CLIENT_FACTORY), e);
    }
    EcsClient client;
    try {
      client = (EcsClient) MethodHandles.lookup()
          .findStatic(clazz, classAndMethod[1], MethodType.methodType(EcsClient.class, Map.class))
          .invoke(properties);
    } catch (Throwable e) {
      throw new IllegalArgumentException(
          String.format("invalid property %s that throw exception", ECS_CLIENT_FACTORY),
          e);
    }
    if (client == null) {
      throw new IllegalArgumentException(String.format(
          "invalid property %s that return null client",
          ECS_CLIENT_FACTORY));
    }
    return Optional.of(client);
  }

  /**
   * get built-in ecs client
   */
  static EcsClient getBuiltInEcsClient(Map<String, String> properties) {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(
                    properties.get(EcsCatalogProperties.ACCESS_KEY_ID),
                    properties.get(EcsCatalogProperties.SECRET_ACCESS_KEY)
                )
            )
        );
    String endpoint = properties.get(EcsCatalogProperties.ENDPOINT);
    if (endpoint != null) {
      builder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(
              endpoint,
              PropertyUtil.propertyAsString(properties, EcsCatalogProperties.REGION, "-")
          )
      );
    } else {
      builder.withRegion(properties.get(EcsCatalogProperties.REGION));
    }

    return new EcsClientImpl(
        builder.build(),
        Collections.unmodifiableMap(new LinkedHashMap<>(properties)),
        new ObjectKeysImpl(EcsCatalogProperties.getObjectBaseKey(properties)),
        PropertiesSerDes.useJdk());
  }
}
