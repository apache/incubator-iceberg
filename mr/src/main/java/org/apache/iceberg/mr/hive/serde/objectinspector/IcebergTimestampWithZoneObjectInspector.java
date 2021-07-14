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

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class IcebergTimestampWithZoneObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements TimestampObjectInspector, WriteObjectInspector {

  private static final IcebergTimestampWithZoneObjectInspector INSTANCE = new IcebergTimestampWithZoneObjectInspector();

  public static IcebergTimestampWithZoneObjectInspector get() {
    return INSTANCE;
  }

  private IcebergTimestampWithZoneObjectInspector() {
    super(TypeInfoFactory.timestampTypeInfo);
  }

  @Override
  public OffsetDateTime convert(Object o) {
    return o == null ? null : OffsetDateTime.of(((Timestamp) o).toLocalDateTime(), ZoneOffset.UTC);
  }

  @Override
  public Timestamp getPrimitiveJavaObject(Object o) {
    return o == null ? null :
        Timestamp.valueOf(((OffsetDateTime) o).withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
  }

  @Override
  public TimestampWritable getPrimitiveWritableObject(Object o) {
    Timestamp ts = getPrimitiveJavaObject(o);
    return ts == null ? null : new TimestampWritable(ts);
  }

  @Override
  public Object copyObject(Object o) {
    if (o instanceof Timestamp) {
      Timestamp ts = (Timestamp) o;
      Timestamp copy = new Timestamp(ts.getTime());
      copy.setNanos(ts.getNanos());
      return copy;
    } else if (o instanceof OffsetDateTime) {
      OffsetDateTime odt = (OffsetDateTime) o;
      return OffsetDateTime.ofInstant(odt.toInstant(), odt.getOffset());
    } else {
      return o;
    }
  }

}
