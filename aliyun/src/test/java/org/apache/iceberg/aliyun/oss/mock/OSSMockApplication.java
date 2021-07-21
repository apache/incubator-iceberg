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

package org.apache.iceberg.aliyun.oss.mock;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SuppressWarnings("checkstyle:AnnotationUseStyle")
@Configuration
@EnableAutoConfiguration(exclude = {SecurityAutoConfiguration.class}, excludeName = {
    "org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration"
})
@ComponentScan
public class OSSMockApplication {

  static final String PROP_ROOT_DIR = "root-dir";

  static final String PROP_HTTP_PORT = "server.port";
  static final int PORT_HTTP_PORT_DEFAULT = 9393;

  static final String PROP_SILENT = "silent";

  @Autowired
  private ConfigurableApplicationContext context;

  @Autowired
  private Config config;

  public static void main(final String[] args) {
    start(Maps.newHashMap(), args);
  }

  public static OSSMockApplication start(Map<String, Object> properties, String... args) {
    Map<String, Object> defaults = Maps.newHashMap();
    defaults.put(PROP_HTTP_PORT, PORT_HTTP_PORT_DEFAULT);

    Banner.Mode bannerMode = Banner.Mode.CONSOLE;

    if (Boolean.parseBoolean(String.valueOf(properties.remove("silent")))) {
      defaults.put("logging.level.root", "WARN");
      bannerMode = Banner.Mode.OFF;
    }

    final ConfigurableApplicationContext ctx =
        new SpringApplicationBuilder(OSSMockApplication.class)
            .properties(defaults)
            .properties(properties)
            .bannerMode(bannerMode)
            .run(args);

    return ctx.getBean(OSSMockApplication.class);
  }

  public void stop() {
    SpringApplication.exit(context, () -> 0);
  }

  @Configuration
  static class Config implements WebMvcConfigurer {

    @Value("${" + PROP_HTTP_PORT + "}")
    private int httpPort;

    @Override
    public void configureContentNegotiation(final ContentNegotiationConfigurer configurer) {
      configurer.defaultContentType(MediaType.APPLICATION_FORM_URLENCODED, MediaType.APPLICATION_XML);
      configurer.favorPathExtension(false);
      configurer.mediaType("xml", MediaType.TEXT_XML);
    }

    @Bean
    Converter<String, Range> rangeConverter() {
      return new RangeConverter();
    }

    /**
     * Creates an HttpMessageConverter for XML.
     *
     * @return The configured {@link MappingJackson2XmlHttpMessageConverter}.
     */
    @Bean
    public MappingJackson2XmlHttpMessageConverter getMessageConverter() {
      List<MediaType> mediaTypes = Lists.newArrayList();
      mediaTypes.add(MediaType.APPLICATION_XML);
      mediaTypes.add(MediaType.APPLICATION_FORM_URLENCODED);
      mediaTypes.add(MediaType.APPLICATION_OCTET_STREAM);

      final MappingJackson2XmlHttpMessageConverter xmlConverter = new MappingJackson2XmlHttpMessageConverter();
      xmlConverter.setSupportedMediaTypes(mediaTypes);

      return xmlConverter;
    }
  }

  private static class RangeConverter implements Converter<String, Range> {

    private static final Pattern REQUESTED_RANGE_PATTERN = Pattern.compile("^bytes=((\\d*)\\-(\\d*))((,\\d*-\\d*)*)");

    @Override
    public Range convert(String rangeString) {
      Preconditions.checkNotNull(rangeString, "Range value should not be null.");

      final Range range;

      // parsing a range specification of format: "bytes=start-end" - multiple ranges not supported
      final Matcher matcher = REQUESTED_RANGE_PATTERN.matcher(rangeString.trim());
      if (matcher.matches()) {
        final String rangeStart = matcher.group(2);
        final String rangeEnd = matcher.group(3);

        long start = StringUtils.isEmpty(rangeStart) ? 0L : Long.parseLong(rangeStart);
        long end = StringUtils.isEmpty(rangeEnd) ? Long.MAX_VALUE : Long.parseLong(rangeEnd);
        range = new Range(start, end);

        if (matcher.groupCount() == 5 && !"".equals(matcher.group(4))) {
          throw new IllegalArgumentException(
              "Unsupported range specification. Only single range specifications allowed");
        }
        if (range.start() < 0) {
          throw new IllegalArgumentException(
              "Unsupported range specification. A start byte must be supplied");
        }

        if (range.end() != -1 && range.end() < range.start()) {
          throw new IllegalArgumentException(
              "Range header is malformed. End byte is smaller than start byte.");
        }
      } else {
        throw new IllegalArgumentException(
            "Range header is malformed. Only bytes supported as range type.");
      }

      return range;
    }
  }
}
