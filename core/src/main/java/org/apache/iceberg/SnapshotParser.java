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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionManagers;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class SnapshotParser {

  private SnapshotParser() {
  }

  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String PARENT_SNAPSHOT_ID = "parent-snapshot-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String SUMMARY = "summary";
  private static final String OPERATION = "operation";
  private static final String MANIFESTS = "manifests";
  private static final String MANIFEST_LIST = "manifest-list";

  static void toJson(Snapshot snapshot, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    if (snapshot.sequenceNumber() > TableMetadata.INITIAL_SEQUENCE_NUMBER) {
      generator.writeNumberField(SEQUENCE_NUMBER, snapshot.sequenceNumber());
    }
    generator.writeNumberField(SNAPSHOT_ID, snapshot.snapshotId());
    if (snapshot.parentId() != null) {
      generator.writeNumberField(PARENT_SNAPSHOT_ID, snapshot.parentId());
    }
    generator.writeNumberField(TIMESTAMP_MS, snapshot.timestampMillis());

    // if there is an operation, write the summary map
    if (snapshot.operation() != null) {
      generator.writeObjectFieldStart(SUMMARY);
      generator.writeStringField(OPERATION, snapshot.operation());
      if (snapshot.summary() != null) {
        for (Map.Entry<String, String> entry : snapshot.summary().entrySet()) {
          // only write operation once
          if (OPERATION.equals(entry.getKey())) {
            continue;
          }
          generator.writeStringField(entry.getKey(), entry.getValue());
        }
      }
      generator.writeEndObject();
    }

    String manifestList = snapshot.manifestListLocation();
    if (manifestList != null) {
      // write just the location. manifests should not be embedded in JSON along with a list
      generator.writeStringField(MANIFEST_LIST, manifestList);
    } else {
      // embed the manifest list in the JSON, v1 only
      generator.writeArrayFieldStart(MANIFESTS);
      for (ManifestFile file : snapshot.allManifests()) {
        generator.writeString(file.path());
      }
      generator.writeEndArray();
    }

    generator.writeEndObject();
  }

  public static String toJson(Snapshot snapshot) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(snapshot, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", snapshot);
    }
  }

  static Snapshot fromJson(FileIO io, EncryptionManager encryption, JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse table version from a non-object: %s", node);

    long sequenceNumber = TableMetadata.INITIAL_SEQUENCE_NUMBER;
    if (node.has(SEQUENCE_NUMBER)) {
      sequenceNumber = JsonUtil.getLong(SEQUENCE_NUMBER, node);
    }
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    Long parentId = null;
    if (node.has(PARENT_SNAPSHOT_ID)) {
      parentId = JsonUtil.getLong(PARENT_SNAPSHOT_ID, node);
    }
    long timestamp = JsonUtil.getLong(TIMESTAMP_MS, node);

    Map<String, String> summary = null;
    String operation = null;
    if (node.has(SUMMARY)) {
      JsonNode sNode = node.get(SUMMARY);
      Preconditions.checkArgument(sNode != null && !sNode.isNull() && sNode.isObject(),
          "Cannot parse summary from non-object value: %s", sNode);

      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      Iterator<String> fields = sNode.fieldNames();
      while (fields.hasNext()) {
        String field = fields.next();
        if (field.equals(OPERATION)) {
          operation = JsonUtil.getString(OPERATION, sNode);
        } else {
          builder.put(field, JsonUtil.getString(field, sNode));
        }
      }
      summary = builder.build();
    }

    if (node.has(MANIFEST_LIST)) {
      // the manifest list is stored in a manifest list file
      String manifestList = JsonUtil.getString(MANIFEST_LIST, node);
      return new BaseSnapshot(io, encryption, sequenceNumber, snapshotId, parentId, timestamp,
          operation, summary, manifestList);

    } else {
      // fall back to an embedded manifest list. pass in the manifest's InputFile so length can be
      // loaded lazily, if it is needed
      List<ManifestFile> manifests = Lists.transform(JsonUtil.getStringList(MANIFESTS, node),
          location -> new GenericManifestFile(io.newInputFile(location), 0));
      return new BaseSnapshot(io, encryption, snapshotId, parentId, timestamp, operation, summary, manifests);
    }
  }

  /**
   * @deprecated please use {@link #fromJson(FileIO, EncryptionManager, JsonNode)}
   */
  @Deprecated
  public static Snapshot fromJson(FileIO io, String json) {
    return fromJson(io, EncryptionManagers.plainText(), json);
  }

  public static Snapshot fromJson(FileIO io, EncryptionManager encryption, String json) {
    try {
      return fromJson(io, encryption, JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read version from json: %s", json);
    }
  }
}
