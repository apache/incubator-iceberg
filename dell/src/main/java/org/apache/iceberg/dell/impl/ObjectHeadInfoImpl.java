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

package org.apache.iceberg.dell.impl;

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.dell.ObjectHeadInfo;

/**
 * a record class of {@link ObjectHeadInfo}
 */
public class ObjectHeadInfoImpl implements ObjectHeadInfo {
  private final long contentLength;
  private final String eTag;
  private final Map<String, String> userMetadata;

  public ObjectHeadInfoImpl(long contentLength, String eTag, Map<String, String> userMetadata) {
    this.contentLength = contentLength;
    this.eTag = eTag;
    this.userMetadata = Collections.unmodifiableMap(userMetadata);
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public String getETag() {
    return eTag;
  }

  @Override
  public Map<String, String> getUserMetadata() {
    return userMetadata;
  }

  @Override
  public String toString() {
    return "ObjectHeadInfoImpl{" +
        "contentLength=" + contentLength +
        ", eTag='" + eTag + '\'' +
        ", userMetadata=" + userMetadata +
        '}';
  }
}
