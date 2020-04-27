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

package org.apache.iceberg.flink.connector.model;

import java.io.ByteArrayOutputStream;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Companion class of CommitMetadata
 */
public class CommitMetadataUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CommitMetadataUtil.class);
  private static final DatumWriter<CommitMetadata> DATUM_WRITER = new SpecificDatumWriter<>(CommitMetadata.class);

  private CommitMetadataUtil() {}

  public static String encodeAsJson(CommitMetadata metadata) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(CommitMetadata.getClassSchema(), outputStream);
      DATUM_WRITER.write(metadata, jsonEncoder);
      jsonEncoder.flush();
      return new String(outputStream.toByteArray());
    } catch (Exception e) {
      LOG.error("failed to encode metadata to JSON", e);
      return "";
    }
  }
}
