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

import org.apache.iceberg.dell.utils.PositionOutputStreamAdapter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * The file impl of {@link InputFile} and {@link OutputFile}
 */
public class EcsFile implements InputFile, OutputFile {

  private final EcsClient ecs;
  private final String location;
  private final ObjectKey key;

  public EcsFile(EcsClient ecs, String location, ObjectKey key) {
    this.ecs = ecs;
    this.location = location;
    this.key = key;
  }

  /**
   * eager-get object length
   *
   * @return length if object exists
   */
  @Override
  public long getLength() {
    return ecs.head(key)
        .orElseThrow(() -> new IllegalStateException(String.format("object not found %s", key)))
        .getContentLength();
  }

  @Override
  public SeekableInputStream newStream() {
    return new EcsSeekableInputStream(ecs, key);
  }

  /**
   * here are some confused things:
   * <p>
   * 1. Should check existence when flush?
   * <p>
   * 2. Should use a placeholder object?
   *
   * @return output stream of object
   */
  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Invalid key");
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return new PositionOutputStreamAdapter(ecs.outputStream(key));
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public InputFile toInputFile() {
    return this;
  }

  /**
   * eager-get object existence
   *
   * @return true if object exists
   */
  @Override
  public boolean exists() {
    return ecs.head(key).isPresent();
  }
}
