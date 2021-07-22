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

package org.apache.iceberg.dell.utils;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.iceberg.io.PositionOutputStream;

/**
 * Adapter of {@link PositionOutputStream} and {@link OutputStream}
 */
public class PositionOutputStreamAdapter extends PositionOutputStream {

  private final OutputStream internal;
  private long pos = 0;

  public PositionOutputStreamAdapter(OutputStream internal) {
    this.internal = internal;
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void write(int b) throws IOException {
    internal.write(b);
    pos += 1;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    internal.write(b, off, len);
    pos += len;
  }

  @Override
  public void flush() throws IOException {
    internal.flush();
  }

  @Override
  public void close() throws IOException {
    internal.close();
  }
}
