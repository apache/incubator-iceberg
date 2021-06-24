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

package org.apache.iceberg.flink.data.vectorized;

import java.util.List;
import java.util.Map;
import org.apache.flink.orc.nohive.vector.OrcNoHiveBytesVector;
import org.apache.flink.orc.nohive.vector.OrcNoHiveDecimalVector;
import org.apache.flink.orc.nohive.vector.OrcNoHiveDoubleVector;
import org.apache.flink.orc.nohive.vector.OrcNoHiveLongVector;
import org.apache.flink.orc.nohive.vector.OrcNoHiveTimestampVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnarArrayData;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.vector.ArrayColumnVector;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.RowColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.orc.OrcBatchReader;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class VectorizedFlinkOrcReaders {
  private VectorizedFlinkOrcReaders() {
  }

  public static OrcBatchReader<VectorizedColumnBatch> buildReader(Schema expectedSchema, TypeDescription fileSchema,
                                                                  Map<Integer, ?> idToConstant) {
    Converter converter = OrcSchemaWithTypeVisitor.visit(expectedSchema, fileSchema, new ReadBuilder(idToConstant));

    return new OrcBatchReader<VectorizedColumnBatch>() {
      // We use batchOffsetInFile to get the correct row offset for current row.
      private long batchOffsetInFile;

      @Override
      public VectorizedColumnBatch read(VectorizedRowBatch batch) {
        FlinkRowColumnVector cv = (FlinkRowColumnVector) converter.convert(
            new StructColumnVector(batch.size, batch.cols), batch.size, batchOffsetInFile);

        VectorizedColumnBatch columnarBatch = new VectorizedColumnBatch(cv.getFieldVectors());
        columnarBatch.setNumRows(batch.size);
        return columnarBatch;
      }

      @Override
      public void setBatchContext(long batchOffsetInFile) {
        this.batchOffsetInFile = batchOffsetInFile;
      }
    };
  }

  private interface Converter {
    ColumnVector convert(org.apache.orc.storage.ql.exec.vector.ColumnVector columnVector, int batchSize,
                         long batchOffsetInFile);
  }

  private static class ReadBuilder extends OrcSchemaWithTypeVisitor<Converter> {
    private final Map<Integer, ?> idToConstant;

    private ReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public Converter record(Types.StructType iStruct, TypeDescription record, List<String> names,
                            List<Converter> fields) {
      return new StructConverter(iStruct, fields, idToConstant);
    }

    @Override
    public Converter list(Types.ListType iList, TypeDescription array, Converter element) {
      return new StructConverter.ArrayConverter(element);
    }

    @Override
    public Converter map(Types.MapType iMap, TypeDescription map, Converter key, Converter value) {
      throw new UnsupportedOperationException("Unsupported vectorized read for map type.");
    }

    @Override
    public Converter primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      return (vector, batchSize, batchOffsetInFile) -> {
        if (vector instanceof LongColumnVector) {
          return new OrcNoHiveLongVector((LongColumnVector) vector);
        } else if (vector instanceof DoubleColumnVector) {
          return new OrcNoHiveDoubleVector((DoubleColumnVector) vector);
        } else if (vector instanceof BytesColumnVector) {
          return new OrcNoHiveBytesVector((BytesColumnVector) vector);
        } else if (vector instanceof DecimalColumnVector) {
          return new OrcNoHiveDecimalVector((DecimalColumnVector) vector);
        } else if (vector instanceof TimestampColumnVector) {
          return new OrcNoHiveTimestampVector((TimestampColumnVector) vector);
        } else {
          throw new UnsupportedOperationException(
              String.format("Unsupported vector: %s, the iceberg type is %s ,the orc type is %s ",
                  vector.getClass().getName(), iPrimitive, primitive));
        }
      };
    }
  }

  private static class RowPositionColumnVector implements org.apache.flink.table.data.vector.LongColumnVector {
    private final long batchOffsetInFile;

    RowPositionColumnVector(long batchOffsetInFile) {
      this.batchOffsetInFile = batchOffsetInFile;
    }

    @Override
    public boolean isNullAt(int i) {
      return false;
    }

    @Override
    public long getLong(int i) {
      return batchOffsetInFile + i;
    }
  }

  private static class StructConverter implements Converter {
    private final Types.StructType structType;
    private final List<Converter> fieldConverters;
    private final Map<Integer, ?> idToConstant;

    private StructConverter(Types.StructType structType, List<Converter> fieldConverters,
                            Map<Integer, ?> idToConstant) {
      this.structType = structType;
      this.fieldConverters = fieldConverters;
      this.idToConstant = idToConstant;
    }

    @Override
    public ColumnVector convert(org.apache.orc.storage.ql.exec.vector.ColumnVector vector, int batchSize,
                                long batchOffsetInFile) {
      StructColumnVector structVector = (StructColumnVector) vector;
      List<Types.NestedField> fields = structType.fields();
      ColumnVector[] fieldVectors = new ColumnVector[fields.size()];
      for (int pos = 0; pos < fields.size(); pos++) {
        Types.NestedField field = fields.get(pos);
        if (idToConstant.containsKey(field.fieldId())) {
          fieldVectors[pos] = toConstantColumnVector(field.type(), idToConstant.get(field.fieldId()));
        } else if (field.equals(MetadataColumns.ROW_POSITION)) {
          fieldVectors[pos] = new RowPositionColumnVector(batchOffsetInFile);
        } else {
          fieldVectors[pos] = fieldConverters.get(pos)
              .convert(structVector.fields[pos], batchSize, batchOffsetInFile);
        }
      }

      return new FlinkRowColumnVector(fieldVectors, structVector);
    }

    private ColumnVector toConstantColumnVector(Type type, Object constant) {
      Type.TypeID typeID = type.typeId();
      switch (typeID) {
        case INTEGER:
        case DATE:
        case TIME:
          return ConstantColumnVectors.ints(constant);

        case LONG:
          return ConstantColumnVectors.longs(constant);

        case BOOLEAN:
          return ConstantColumnVectors.booleans(constant);

        case DOUBLE:
          return ConstantColumnVectors.doubles(constant);

        case FLOAT:
          return ConstantColumnVectors.floats(constant);

        case DECIMAL:
          return ConstantColumnVectors.decimals(constant);

        case TIMESTAMP:
          return ConstantColumnVectors.timestamps(constant);

        case FIXED:
        case UUID:
        case BINARY:
        case STRING:
          return ConstantColumnVectors.bytes(constant);

        default:
          throw new UnsupportedOperationException("Unsupported data type for constant.");
      }
    }

    private static class ArrayConverter implements Converter {
      private final Converter elementConverter;

      private ArrayConverter(Converter elementConverter) {
        this.elementConverter = elementConverter;
      }

      @Override
      public ColumnVector convert(org.apache.orc.storage.ql.exec.vector.ColumnVector vector, int batchSize,
                                  long batchOffsetInFile) {
        ListColumnVector listVector = (ListColumnVector) vector;
        ColumnVector elementVector = elementConverter.convert(listVector.child, batchSize, batchOffsetInFile);

        return new ArrayColumnVector() {
          @Override
          public ArrayData getArray(int rowId) {
            int index = getRowIndex(rowId);
            return new ColumnarArrayData(elementVector, (int) listVector.offsets[index],
                (int) listVector.lengths[index]);
          }

          @Override
          public boolean isNullAt(int rowId) {
            return vector.isNull[getRowIndex(rowId)];
          }

          private int getRowIndex(int rowId) {
            return vector.isRepeating ? 0 : rowId;
          }
        };
      }
    }
  }

  private static class FlinkRowColumnVector implements RowColumnVector {

    private final ColumnVector[] fieldVectors;
    private final StructColumnVector structVector;
    private final VectorizedColumnBatch vectorizedColumnBatch;

    FlinkRowColumnVector(ColumnVector[] fieldVectors,
                         StructColumnVector structVector) {
      this.fieldVectors = fieldVectors;
      this.structVector = structVector;
      this.vectorizedColumnBatch = new VectorizedColumnBatch(fieldVectors);
    }

    @Override
    public ColumnarRowData getRow(int i) {
      return new ColumnarRowData(vectorizedColumnBatch, i);
    }

    @Override
    public boolean isNullAt(int i) {
      return structVector.isNull[i];
    }

    ColumnVector[] getFieldVectors() {
      return fieldVectors;
    }
  }
}
