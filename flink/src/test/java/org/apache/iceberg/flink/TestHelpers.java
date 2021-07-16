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

package org.apache.iceberg.flink;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkInputSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;

public class TestHelpers {
  private TestHelpers() {
  }

  public static RowData copyRowData(RowData from, RowType rowType) {
    TypeSerializer[] fieldSerializers = rowType.getChildren().stream()
        .map((LogicalType type) -> InternalSerializers.create(type))
        .toArray(TypeSerializer[]::new);
    return RowDataUtil.clone(from, null, rowType, fieldSerializers);
  }

  public static List<RowData> readRowData(FlinkInputFormat inputFormat, RowType rowType) throws IOException {
    FlinkInputSplit[] splits = inputFormat.createInputSplits(0);
    List<RowData> results = Lists.newArrayList();

    for (FlinkInputSplit s : splits) {
      inputFormat.open(s);
      while (!inputFormat.reachedEnd()) {
        RowData row = inputFormat.nextRecord(null);
        results.add(copyRowData(row, rowType));
      }
    }
    inputFormat.close();

    return results;
  }

  public static List<Row> readRows(FlinkInputFormat inputFormat, RowType rowType) throws IOException {
    return convertRowDataToRow(readRowData(inputFormat, rowType), rowType);
  }

  public static List<Row> convertRowDataToRow(List<RowData> rowDataList, RowType rowType) {
    DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(rowType));
    return rowDataList.stream()
        .map(converter::toExternal)
        .map(Row.class::cast)
        .collect(Collectors.toList());
  }

  public static void assertRecords(List<Row> results, List<Record> expectedRecords, Schema schema) {
    List<Row> expected = Lists.newArrayList();
    @SuppressWarnings("unchecked")
    DataStructureConverter<RowData, Row> converter = (DataStructureConverter) DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(FlinkSchemaUtil.convert(schema)));
    expectedRecords.forEach(r -> expected.add(converter.toExternal(RowDataConverter.convert(schema, r))));
    assertRows(results, expected);
  }

  public static void assertRows(List<Row> results, List<Row> expected) {
    expected.sort(Comparator.comparing(Row::toString));
    results.sort(Comparator.comparing(Row::toString));
    Assert.assertEquals(expected, results);
  }

  public static void assertRowData(Schema schema, StructLike expected, RowData actual) {
    assertRowData(schema.asStruct(), FlinkSchemaUtil.convert(schema), expected, actual);
  }

  public static void assertRowData(Types.StructType structType, LogicalType rowType, StructLike expectedRecord,
                                   RowData actualRowData) {
    if (expectedRecord == null && actualRowData == null) {
      return;
    }

    Assert.assertTrue("expected Record and actual RowData should be both null or not null",
        expectedRecord != null && actualRowData != null);

    List<Type> types = Lists.newArrayList();
    for (Types.NestedField field : structType.fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      LogicalType logicalType = ((RowType) rowType).getTypeAt(i);
      Object expected = expectedRecord.get(i, Object.class);
      // The RowData.createFieldGetter won't return null for the required field. But in the projection case, if we are
      // projecting a nested required field from a optional struct, then we should give a null for the projected field
      // if the outer struct value is null. So we need to check the nullable for actualRowData here. For more details
      // please see issue #2738.
      Object actual = actualRowData.isNullAt(i) ? null :
          RowData.createFieldGetter(logicalType, i).getFieldOrNull(actualRowData);
      assertEquals(types.get(i), logicalType, expected, actual);
    }
  }

  private static void assertEquals(Type type, LogicalType logicalType, Object expected, Object actual) {

    if (expected == null && actual == null) {
      return;
    }

    Assert.assertTrue("expected and actual should be both null or not null",
        expected != null && actual != null);

    switch (type.typeId()) {
      case BOOLEAN:
        Assert.assertEquals("boolean value should be equal", expected, actual);
        break;
      case INTEGER:
        Assert.assertEquals("int value should be equal", expected, actual);
        break;
      case LONG:
        Assert.assertEquals("long value should be equal", expected, actual);
        break;
      case FLOAT:
        Assert.assertEquals("float value should be equal", expected, actual);
        break;
      case DOUBLE:
        Assert.assertEquals("double value should be equal", expected, actual);
        break;
      case STRING:
        Assert.assertTrue("Should expect a CharSequence", expected instanceof CharSequence);
        Assert.assertEquals("string should be equal", String.valueOf(expected), actual.toString());
        break;
      case DATE:
        Assert.assertTrue("Should expect a Date", expected instanceof LocalDate);
        LocalDate date = DateTimeUtil.dateFromDays((int) actual);
        Assert.assertEquals("date should be equal", expected, date);
        break;
      case TIME:
        Assert.assertTrue("Should expect a LocalTime", expected instanceof LocalTime);
        int milliseconds = (int) (((LocalTime) expected).toNanoOfDay() / 1000_000);
        Assert.assertEquals("time millis should be equal", milliseconds, actual);
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          Assert.assertTrue("Should expect a OffsetDataTime", expected instanceof OffsetDateTime);
          OffsetDateTime ts = (OffsetDateTime) expected;
          Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
              ((TimestampData) actual).toLocalDateTime());
        } else {
          Assert.assertTrue("Should expect a LocalDataTime", expected instanceof LocalDateTime);
          LocalDateTime ts = (LocalDateTime) expected;
          Assert.assertEquals("LocalDataTime should be equal", ts,
              ((TimestampData) actual).toLocalDateTime());
        }
        break;
      case BINARY:
        Assert.assertTrue("Should expect a ByteBuffer", expected instanceof ByteBuffer);
        Assert.assertEquals("binary should be equal", expected, ByteBuffer.wrap((byte[]) actual));
        break;
      case DECIMAL:
        Assert.assertTrue("Should expect a BigDecimal", expected instanceof BigDecimal);
        BigDecimal bd = (BigDecimal) expected;
        Assert.assertEquals("decimal value should be equal", bd,
            ((DecimalData) actual).toBigDecimal());
        break;
      case LIST:
        Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
        Collection<?> expectedArrayData = (Collection<?>) expected;
        ArrayData actualArrayData = (ArrayData) actual;
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        Assert.assertEquals("array length should be equal", expectedArrayData.size(), actualArrayData.size());
        assertArrayValues(type.asListType().elementType(), elementType, expectedArrayData, actualArrayData);
        break;
      case MAP:
        Assert.assertTrue("Should expect a Map", expected instanceof Map);
        assertMapValues(type.asMapType(), logicalType, (Map<?, ?>) expected, (MapData) actual);
        break;
      case STRUCT:
        Assert.assertTrue("Should expect a Record", expected instanceof StructLike);
        assertRowData(type.asStructType(), logicalType, (StructLike) expected, (RowData) actual);
        break;
      case UUID:
        Assert.assertTrue("Should expect a UUID", expected instanceof UUID);
        Assert.assertEquals("UUID should be equal", expected.toString(),
            UUID.nameUUIDFromBytes((byte[]) actual).toString());
        break;
      case FIXED:
        Assert.assertTrue("Should expect byte[]", expected instanceof byte[]);
        Assert.assertArrayEquals("binary should be equal", (byte[]) expected, (byte[]) actual);
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  private static void assertArrayValues(Type type, LogicalType logicalType, Collection<?> expectedArray,
                                        ArrayData actualArray) {
    List<?> expectedElements = Lists.newArrayList(expectedArray);
    for (int i = 0; i < expectedArray.size(); i += 1) {
      if (expectedElements.get(i) == null) {
        Assert.assertTrue(actualArray.isNullAt(i));
        continue;
      }

      Object expected = expectedElements.get(i);

      assertEquals(type, logicalType, expected,
          ArrayData.createElementGetter(logicalType).getElementOrNull(actualArray, i));
    }
  }

  private static void assertMapValues(Types.MapType mapType, LogicalType type, Map<?, ?> expected, MapData actual) {
    Assert.assertEquals("map size should be equal", expected.size(), actual.size());

    ArrayData actualKeyArrayData = actual.keyArray();
    ArrayData actualValueArrayData = actual.valueArray();
    LogicalType actualKeyType = ((MapType) type).getKeyType();
    LogicalType actualValueType = ((MapType) type).getValueType();
    Type keyType = mapType.keyType();
    Type valueType = mapType.valueType();

    ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(actualKeyType);
    ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(actualValueType);

    for (Map.Entry<?, ?> entry : expected.entrySet()) {
      Object matchedActualKey = null;
      int matchedKeyIndex = 0;
      for (int i = 0; i < actual.size(); i += 1) {
        try {
          Object key = keyGetter.getElementOrNull(actualKeyArrayData, i);
          assertEquals(keyType, actualKeyType, entry.getKey(), key);
          matchedActualKey = key;
          matchedKeyIndex = i;
          break;
        } catch (AssertionError e) {
          // not found
        }
      }
      Assert.assertNotNull("Should have a matching key", matchedActualKey);
      final int valueIndex = matchedKeyIndex;
      assertEquals(valueType, actualValueType, entry.getValue(),
          valueGetter.getElementOrNull(actualValueArrayData, valueIndex));
    }
  }

  public static void assertEquals(ManifestFile expected, ManifestFile actual) {
    if (expected == actual) {
      return;
    }
    Assert.assertTrue("Should not be null.", expected != null && actual != null);
    Assert.assertEquals("Path must match", expected.path(), actual.path());
    Assert.assertEquals("Length must match", expected.length(), actual.length());
    Assert.assertEquals("Spec id must match", expected.partitionSpecId(), actual.partitionSpecId());
    Assert.assertEquals("ManifestContent must match", expected.content(), actual.content());
    Assert.assertEquals("SequenceNumber must match", expected.sequenceNumber(), actual.sequenceNumber());
    Assert.assertEquals("MinSequenceNumber must match", expected.minSequenceNumber(), actual.minSequenceNumber());
    Assert.assertEquals("Snapshot id must match", expected.snapshotId(), actual.snapshotId());
    Assert.assertEquals("Added files flag must match", expected.hasAddedFiles(), actual.hasAddedFiles());
    Assert.assertEquals("Added files count must match", expected.addedFilesCount(), actual.addedFilesCount());
    Assert.assertEquals("Added rows count must match", expected.addedRowsCount(), actual.addedRowsCount());
    Assert.assertEquals("Existing files flag must match", expected.hasExistingFiles(), actual.hasExistingFiles());
    Assert.assertEquals("Existing files count must match", expected.existingFilesCount(), actual.existingFilesCount());
    Assert.assertEquals("Existing rows count must match", expected.existingRowsCount(), actual.existingRowsCount());
    Assert.assertEquals("Deleted files flag must match", expected.hasDeletedFiles(), actual.hasDeletedFiles());
    Assert.assertEquals("Deleted files count must match", expected.deletedFilesCount(), actual.deletedFilesCount());
    Assert.assertEquals("Deleted rows count must match", expected.deletedRowsCount(), actual.deletedRowsCount());

    List<ManifestFile.PartitionFieldSummary> expectedSummaries = expected.partitions();
    List<ManifestFile.PartitionFieldSummary> actualSummaries = actual.partitions();
    Assert.assertEquals("PartitionFieldSummary size does not match", expectedSummaries.size(), actualSummaries.size());
    for (int i = 0; i < expectedSummaries.size(); i++) {
      Assert.assertEquals("Null flag in partition must match",
          expectedSummaries.get(i).containsNull(), actualSummaries.get(i).containsNull());
      Assert.assertEquals("NaN flag in partition must match",
          expectedSummaries.get(i).containsNaN(), actualSummaries.get(i).containsNaN());
      Assert.assertEquals("Lower bounds in partition must match",
          expectedSummaries.get(i).lowerBound(), actualSummaries.get(i).lowerBound());
      Assert.assertEquals("Upper bounds in partition must match",
          expectedSummaries.get(i).upperBound(), actualSummaries.get(i).upperBound());
    }
  }

  public static void assertEquals(ContentFile<?> expected, ContentFile<?> actual) {
    if (expected == actual) {
      return;
    }
    Assert.assertTrue("Shouldn't be null.", expected != null && actual != null);
    Assert.assertEquals("SpecId", expected.specId(), actual.specId());
    Assert.assertEquals("Content", expected.content(), actual.content());
    Assert.assertEquals("Path", expected.path(), actual.path());
    Assert.assertEquals("Format", expected.format(), actual.format());
    Assert.assertEquals("Partition size", expected.partition().size(), actual.partition().size());
    for (int i = 0; i < expected.partition().size(); i++) {
      Assert.assertEquals("Partition data at index " + i,
          expected.partition().get(i, Object.class),
          actual.partition().get(i, Object.class));
    }
    Assert.assertEquals("Record count", expected.recordCount(), actual.recordCount());
    Assert.assertEquals("File size in bytes", expected.fileSizeInBytes(), actual.fileSizeInBytes());
    Assert.assertEquals("Column sizes", expected.columnSizes(), actual.columnSizes());
    Assert.assertEquals("Value counts", expected.valueCounts(), actual.valueCounts());
    Assert.assertEquals("Null value counts", expected.nullValueCounts(), actual.nullValueCounts());
    Assert.assertEquals("Lower bounds", expected.lowerBounds(), actual.lowerBounds());
    Assert.assertEquals("Upper bounds", expected.upperBounds(), actual.upperBounds());
    Assert.assertEquals("Key metadata", expected.keyMetadata(), actual.keyMetadata());
    Assert.assertEquals("Split offsets", expected.splitOffsets(), actual.splitOffsets());
    Assert.assertEquals("Equality field id list", actual.equalityFieldIds(), expected.equalityFieldIds());
  }
}
