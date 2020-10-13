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

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergTimestampObjectInspectorHive3 {

  @Test
  public void testIcebergTimestampObjectInspector() {
    TimestampObjectInspector oi = IcebergTimestampObjectInspectorHive3.get(false);

    Assert.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assert.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, oi.getPrimitiveCategory());

    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo.getTypeName(), oi.getTypeName());

    Assert.assertEquals(Timestamp.class, oi.getJavaPrimitiveClass());
    Assert.assertEquals(TimestampWritableV2.class, oi.getPrimitiveWritableClass());

    Assert.assertNull(oi.copyObject(null));
    Assert.assertNull(oi.getPrimitiveJavaObject(null));
    Assert.assertNull(oi.getPrimitiveWritableObject(null));

    long epochMilli = 1601471970000L;
    LocalDateTime local = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.of("UTC"));
    Timestamp ts = Timestamp.ofEpochMilli(epochMilli);

    Assert.assertEquals(ts, oi.getPrimitiveJavaObject(local));
    Assert.assertEquals(new TimestampWritableV2(ts), oi.getPrimitiveWritableObject(local));

    Timestamp copy = (Timestamp) oi.copyObject(ts);

    Assert.assertEquals(ts, copy);
    Assert.assertNotSame(ts, copy);

    Assert.assertFalse(oi.preferWritable());
  }

  @Test
  public void testIcebergTimestampObjectInspectorWithUTCAdjustment() {
    TimestampObjectInspector oi = IcebergTimestampObjectInspectorHive3.get(true);

    Assert.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
    Assert.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, oi.getPrimitiveCategory());

    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo.getTypeName(), oi.getTypeName());

    Assert.assertEquals(Timestamp.class, oi.getJavaPrimitiveClass());
    Assert.assertEquals(TimestampWritableV2.class, oi.getPrimitiveWritableClass());

    Assert.assertNull(oi.copyObject(null));
    Assert.assertNull(oi.getPrimitiveJavaObject(null));
    Assert.assertNull(oi.getPrimitiveWritableObject(null));

    long epochMilli = 1601471970000L;
    LocalDateTime local = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.of("UTC"));
    OffsetDateTime offsetDateTime = OffsetDateTime.of(local, ZoneOffset.ofHours(4));
    Timestamp ts = Timestamp.ofEpochMilli(epochMilli);

    Assert.assertEquals(ts, oi.getPrimitiveJavaObject(offsetDateTime));
    Assert.assertEquals(new TimestampWritableV2(ts), oi.getPrimitiveWritableObject(offsetDateTime));

    Timestamp copy = (Timestamp) oi.copyObject(ts);

    Assert.assertEquals(ts, copy);
    Assert.assertNotSame(ts, copy);

    Assert.assertFalse(oi.preferWritable());
  }

}
