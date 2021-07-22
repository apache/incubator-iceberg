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

import org.apache.iceberg.dell.EcsClient;
import org.apache.iceberg.dell.ObjectHeadInfo;

/**
 * a record class of {@link EcsClient.ContentAndETag}
 */
public class ContentAndETagImpl implements EcsClient.ContentAndETag {

  private final ObjectHeadInfo headInfo;
  private final byte[] content;

  public ContentAndETagImpl(ObjectHeadInfo headInfo, byte[] content) {
    this.headInfo = headInfo;
    this.content = content;
  }

  @Override
  public ObjectHeadInfo getHeadInfo() {
    return headInfo;
  }

  @Override
  public byte[] getContent() {
    return content;
  }
}
