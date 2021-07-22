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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * convert Map properties to bytes.
 */
public interface PropertiesSerDes {

  /**
   * version of properties.
   * <p>
   * this properties is only a placeholder and may use in future.
   */
  String CURRENT_VERSION = "0";

  Logger log = LoggerFactory.getLogger(PropertiesSerDes.class);

  /**
   * read properties from stream
   *
   * @param input is stream
   * @return properties
   */
  Map<String, String> read(InputStream input);

  /**
   * a utils to read properties from object content.
   *
   * @param content is content of object
   * @param eTag    is eTag of object
   * @param version is property version (current field is a placeholder for future changes)
   * @return properties which loaded by content
   */
  default Map<String, String> readProperties(byte[] content, String eTag, String version) {
    Map<String, String> propertiesInObject = read(new ByteArrayInputStream(content));
    Map<String, String> properties = new HashMap<>(propertiesInObject);
    properties.put(EcsClient.E_TAG_KEY, eTag);
    properties.put(EcsClient.PROPERTY_VERSION_KEY, version);
    return properties;
  }

  /**
   * write properties to bytes.
   *
   * @param value is properties
   * @return bytes content
   */
  byte[] toBytes(Map<String, String> value);

  /**
   * use {@link Properties} to serialize and deserialize properties.
   *
   * @return interface
   */
  static PropertiesSerDes useJdk() {
    return new PropertiesSerDes() {
      @Override
      public Map<String, String> read(InputStream input) {
        Properties jdkProperties = new Properties();
        try {
          jdkProperties.load(new InputStreamReader(input, StandardCharsets.UTF_8));
        } catch (IOException e) {
          log.error("fail to read properties", e);
          throw new UncheckedIOException(e);
        }
        Set<String> propertyNames = jdkProperties.stringPropertyNames();
        Map<String, String> properties = new HashMap<>();
        for (String name : propertyNames) {
          properties.put(name, jdkProperties.getProperty(name));
        }
        return Collections.unmodifiableMap(properties);
      }

      @Override
      public byte[] toBytes(Map<String, String> value) {
        Properties jdkProperties = new Properties();
        for (Map.Entry<String, String> entry : value.entrySet()) {
          jdkProperties.setProperty(entry.getKey(), entry.getValue());
        }
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
          jdkProperties.store(new OutputStreamWriter(output, StandardCharsets.UTF_8), null);
          return output.toByteArray();
        } catch (IOException e) {
          log.error("fail to store properties {} to file", value, e);
          throw new UncheckedIOException(e);
        }
      }
    };
  }
}
