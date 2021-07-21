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

import java.util.Map;

public class ObjectMetadata {

  private long contentLength;

  // In millis
  private long lastModificationDate;

  private String contentMD5;

  private String contentType;

  private String contentEncoding;

  private Map<String, String> userMetaData;

  private String dataFile;

  private String metaFile;

  // The following getters and setters are required for Jackson ObjectMapper serialization and deserialization.

  public long getContentLength() {
    return contentLength;
  }

  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }

  public long getLastModificationDate() {
    return lastModificationDate;
  }

  public void setLastModificationDate(long lastModificationDate) {
    this.lastModificationDate = lastModificationDate;
  }

  public String getContentMD5() {
    return contentMD5;
  }

  public void setContentMD5(String contentMD5) {
    this.contentMD5 = contentMD5;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  public void setContentEncoding(String contentEncoding) {
    this.contentEncoding = contentEncoding;
  }

  public Map<String, String> getUserMetaData() {
    return userMetaData;
  }

  public void setUserMetaData(Map<String, String> userMetaData) {
    this.userMetaData = userMetaData;
  }

  public String getDataFile() {
    return dataFile;
  }

  public void setDataFile(String dataFile) {
    this.dataFile = dataFile;
  }

  public String getMetaFile() {
    return metaFile;
  }

  public void setMetaFile(String metaFile) {
    this.metaFile = metaFile;
  }
}
