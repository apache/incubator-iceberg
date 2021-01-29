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

package org.apache.iceberg.beam;

import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.test.Cat;
import org.junit.Test;

public class FileWriterTest extends BaseTest {

  @Test
  public void testWriteFiles() {
    for (FileFormat format : FILEFORMATS) {
      writeFile(format);
    }
  }

  public void writeFile(FileFormat fileFormat) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(Cat.getClassSchema());
    TableIdentifier name = TableIdentifier.of("default", "test_file_writer_" + fileFormat.name());

    FileWriter<Cat> f = new FileWriter<>(
        name,
        icebergSchema,
        PartitionSpec.unpartitioned(),
        hiveMetastoreUrl,
        Maps.newHashMap()
    );

    f.start();

    for (Cat cat : specificCats) {
      f.appendRecord(cat, null, 0, 0);
    }

    DataFile[] files = f.finish();

    assert (files.length == 1);
  }
}
