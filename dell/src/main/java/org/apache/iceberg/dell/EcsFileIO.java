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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link java.io.Externalizable} FileIO that manage the life cycle of {@link EcsClient}
 */
public class EcsFileIO implements FileIO, Externalizable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(EcsFileIO.class);

  private EcsClient ecs;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * constructor for {@link Externalizable}
   */
  public EcsFileIO() {
  }

  /**
   * constructor
   *
   * @param ecs is an ecs client
   */
  public EcsFileIO(EcsClient ecs) {
    this.ecs = ecs;
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      log.info("close s3 file io");
      ecs.close();
    }
  }

  @Override
  public InputFile newInputFile(String path) {
    checkOpen();
    return new EcsFile(ecs, path, ecs.getKeys().parse(path));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    checkOpen();
    return new EcsFile(ecs, path, ecs.getKeys().parse(path));
  }

  @Override
  public void deleteFile(String path) {
    checkOpen();
    ecs.deleteObject(ecs.getKeys().parse(path));
  }

  /**
   * manual serialize object
   *
   * @param out is object output stream
   * @throws IOException if exceptional
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(ecs.getProperties());
  }

  /**
   * manual deserialize object
   *
   * @param in is object input stream
   * @throws IOException            if exceptional
   * @throws ClassNotFoundException if exceptional
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    @SuppressWarnings("unchecked")
    Map<String, String> properties = (Map<String, String>) in.readObject();
    ecs = EcsClient.create(properties);
  }

  private void checkOpen() {
    if (closed.get()) {
      throw new IllegalStateException("file io is closed");
    }
  }
}
