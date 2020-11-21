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

package org.apache.iceberg.io;

import java.io.IOException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;

public class UnpartitionedDeltaWriter<T> extends BaseTaskWriter<T> {

  private final RollingFileWriter currentWriter;
  private final RollingEqDeleteWriter currentEqDeletes;

  public UnpartitionedDeltaWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                                  OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    currentWriter = new RollingFileWriter(null);
    currentEqDeletes = new RollingEqDeleteWriter(null);
  }

  @Override
  public void write(T record) throws IOException {
    currentWriter.add(record);
  }

  public void delete(T record) throws IOException {
    currentEqDeletes.delete(record);
  }

  @Override
  public void close() throws IOException {
    currentWriter.close();
    currentEqDeletes.close();
  }
}