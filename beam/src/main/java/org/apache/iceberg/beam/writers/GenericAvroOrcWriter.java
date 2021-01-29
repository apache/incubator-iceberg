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

package org.apache.iceberg.beam.writers;

import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenericAvroOrcWriter implements OrcRowWriter<GenericRecord> {
  private final OrcValueWriter writer;

  private GenericAvroOrcWriter(Schema expectedSchema, TypeDescription orcSchema) {
    Preconditions.checkArgument(orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "Top level must be a struct " + orcSchema);

    writer = OrcSchemaWithTypeVisitor.visit(expectedSchema, orcSchema, new WriteBuilder());
  }

  public static OrcRowWriter<GenericRecord> buildWriter(Schema expectedSchema, TypeDescription fileSchema) {
    return new GenericAvroOrcWriter(expectedSchema, fileSchema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(GenericRecord value, VectorizedRowBatch output) {
    Preconditions.checkArgument(writer instanceof RecordWriter, "writer must be a RecordWriter.");

    int row = output.size;
    output.size += 1;
    List<OrcValueWriter<?>> writers = ((RecordWriter) writer).writers();
    for (int c = 0; c < writers.size(); ++c) {
      OrcValueWriter child = writers.get(c);
      Object fieldValue = value.get(c);
      if (fieldValue instanceof Utf8) {
        fieldValue = ((Utf8) fieldValue).toString();
      }
      child.write(row, fieldValue, output.cols[c]);
    }
  }

  private static class WriteBuilder extends OrcSchemaWithTypeVisitor<OrcValueWriter<?>> {
    private WriteBuilder() {
    }

    @Override
    public OrcValueWriter<GenericRecord> record(Types.StructType iStruct, TypeDescription record,
                             List<String> names, List<OrcValueWriter<?>> fields) {
      return new RecordWriter(fields);
    }

    @Override
    public OrcValueWriter<?> list(Types.ListType iList, TypeDescription array,
                    OrcValueWriter<?> element) {
      return GenericOrcWriters.list(element);
    }

    @Override
    public OrcValueWriter<?> map(Types.MapType iMap, TypeDescription map,
                   OrcValueWriter<?> key, OrcValueWriter<?> value) {
      return GenericOrcWriters.map(key, value);
    }

    @Override
    public OrcValueWriter<?> primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (iPrimitive.typeId()) {
        case BOOLEAN:
          return GenericOrcWriters.booleans();
        case INTEGER:
          return GenericOrcWriters.ints();
        case LONG:
          return GenericOrcWriters.longs();
        case FLOAT:
          return GenericOrcWriters.floats();
        case DOUBLE:
          return GenericOrcWriters.doubles();
        case DATE:
          return GenericOrcWriters.dates();
        case TIME:
          return GenericOrcWriters.times();
        case TIMESTAMP:
          Types.TimestampType timestampType = (Types.TimestampType) iPrimitive;
          if (timestampType.shouldAdjustToUTC()) {
            return GenericOrcWriters.timestampTz();
          } else {
            return GenericOrcWriters.timestamp();
          }
        case STRING:
          return GenericOrcWriters.strings();
        case UUID:
          return GenericOrcWriters.uuids();
        case FIXED:
          return GenericOrcWriters.byteArrays();
        case BINARY:
          return GenericOrcWriters.byteBuffers();
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) iPrimitive;
          return GenericOrcWriters.decimal(decimalType.precision(), decimalType.scale());
        default:
          throw new IllegalArgumentException(String.format("Invalid iceberg type %s corresponding to ORC type %s",
              iPrimitive, primitive));
      }
    }
  }

  private static class RecordWriter implements OrcValueWriter<GenericRecord> {
    private final List<OrcValueWriter<?>> writers;

    RecordWriter(List<OrcValueWriter<?>> writers) {
      this.writers = writers;
    }

    List<OrcValueWriter<?>> writers() {
      return writers;
    }

    @Override
    public Class<GenericRecord> getJavaClass() {
      return GenericRecord.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void nonNullWrite(int rowId, GenericRecord data, ColumnVector output) {
      StructColumnVector cv = (StructColumnVector) output;
      for (int c = 0; c < writers.size(); ++c) {
        OrcValueWriter child = writers.get(c);
        child.write(rowId, data.get(c), cv.fields[c]);
      }
    }
  }
}
