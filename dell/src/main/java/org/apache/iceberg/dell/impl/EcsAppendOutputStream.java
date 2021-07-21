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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iceberg.dell.ObjectKey;

/**
 * use ECS append api to write data
 */
public class EcsAppendOutputStream extends OutputStream {

  private final AmazonS3 s3;

  private final ObjectKey key;

  /**
   * local bytes cache that avoid too many requests
   * <p>
   * use {@link ByteBuffer} to maintain offset
   */
  private final ByteBuffer localCache;

  private boolean firstPart = true;

  public EcsAppendOutputStream(AmazonS3 s3, ObjectKey key, byte[] localCache) {
    this.s3 = s3;
    this.key = key;
    this.localCache = ByteBuffer.wrap(localCache);
  }

  @Override
  public void write(int b) {
    if (!checkBuffer(1)) {
      flush();
    }
    localCache.put((byte) b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    if (!checkBuffer(len)) {
      flush();
    }
    if (checkBuffer(len)) {
      localCache.put(b, off, len);
    } else {
      // if content > cache, directly flush itself.
      flushBuffer(b, off, len);
    }
  }

  private boolean checkBuffer(int nextWrite) {
    return localCache.remaining() >= nextWrite;
  }

  private void flushBuffer(byte[] buffer, int offset, int length) {
    ObjectMetadata metadata = new ObjectMetadata();
    if (firstPart) {
      firstPart = false;
    } else {
      // only following parts need append api
      metadata.setHeader(Headers.RANGE, "bytes=-1-");
    }
    metadata.setContentLength(length);
    s3.putObject(key.getBucket(), key.getKey(), new ByteArrayInputStream(buffer, offset, length), metadata);
  }

  /**
   * flush all cached bytes
   */
  @Override
  public void flush() {
    if (localCache.remaining() < localCache.capacity()) {
      localCache.flip();
      flushBuffer(localCache.array(), localCache.arrayOffset(), localCache.remaining());
      localCache.clear();
    }
  }

  @Override
  public void close() {
    // call flush to guarantee all bytes are submitted
    flush();
  }
}
