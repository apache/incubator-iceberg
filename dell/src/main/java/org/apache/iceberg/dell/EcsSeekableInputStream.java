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

import java.io.IOException;
import java.io.InputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * {@link SeekableInputStream} impl that warp {@link EcsClient#inputStream(ObjectKey, long)}
 * <p>
 * 1. the stream is only be loaded when start reading.
 * <p>
 * 2. this class won't catch any bytes of content. It only maintains pos of {@link SeekableInputStream}
 * <p>
 * 3. this class is not thread-safe.
 */
public class EcsSeekableInputStream extends SeekableInputStream {

  private final EcsClient ecs;
  private final ObjectKey key;

  /**
   * mutable pos set by {@link #seek(long)}
   */
  private long newPos = 0;
  /**
   * current pos of object content
   */
  private long pos = -1;
  private InputStream internal;

  public EcsSeekableInputStream(EcsClient ecs, ObjectKey key) {
    this.ecs = ecs;
    this.key = key;
  }

  @Override
  public long getPos() {
    return newPos >= 0 ? newPos : pos;
  }

  @Override
  public void seek(long inputNewPos) {
    if (pos == inputNewPos) {
      return;
    }
    newPos = inputNewPos;
  }

  @Override
  public int read() throws IOException {
    syncNewPosToPos();
    pos += 1;
    return internal.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    syncNewPosToPos();
    int delta = internal.read(b, off, len);
    pos += delta;
    return delta;
  }

  private void syncNewPosToPos() throws IOException {
    if (newPos < 0) {
      return;
    }
    if (newPos == pos) {
      newPos = -1;
      return;
    }
    if (internal != null) {
      internal.close();
    }
    pos = newPos;
    internal = ecs.inputStream(key, pos);
    newPos = -1;
  }

  @Override
  public void close() throws IOException {
    if (internal != null) {
      internal.close();
    }
  }
}
