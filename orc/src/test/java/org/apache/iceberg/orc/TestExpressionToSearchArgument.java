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

package org.apache.iceberg.orc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf.Type;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestExpressionToSearchArgument {

  @Test
  public void testPrimitiveTypes() {
    Schema schema = new Schema(
        required(1, "int", Types.IntegerType.get()),
        required(2, "long", Types.LongType.get()),
        required(3, "float", Types.FloatType.get()),
        required(4, "double", Types.DoubleType.get()),
        required(5, "boolean", Types.BooleanType.get()),
        required(6, "string", Types.StringType.get()),
        required(7, "date", Types.DateType.get()),
        required(8, "time", Types.TimeType.get()),
        required(9, "tsTz", Types.TimestampType.withZone()),
        required(10, "ts", Types.TimestampType.withoutZone()),
        required(11, "decimal", Types.DecimalType.of(38, 2))
    );

    Expression expr = and(
        and(
            and(lessThan("int", 1), lessThanOrEqual("long", 100)),
            and(greaterThan("float", 5.0), greaterThanOrEqual("double", 500.0))
        ),
        and(
            and(equal("boolean", true), notEqual("string", "test")),
            and(in("decimal", BigDecimal.valueOf(-12345, 2), BigDecimal.valueOf(12345, 2)), notIn("time", 100L, 200L))
        )
    );
    Expression boundFilter = Binder.bind(schema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("`int`", Type.LONG, 1L)
        .lessThanEquals("`long`", Type.LONG, 100L)
        .startNot().lessThanEquals("`float`", Type.FLOAT, 5.0).end()
        .startNot().lessThan("`double`", Type.FLOAT, 500.0).end()
        .equals("`boolean`", Type.BOOLEAN, true)
        .startOr().isNull("`string`", Type.STRING).startNot().equals("`string`", Type.STRING, "test").end().end()
        .in("`decimal`", Type.DECIMAL, new HiveDecimalWritable("-123.45"), new HiveDecimalWritable("123.45"))
        .startOr().isNull("`time`", Type.LONG).startNot().in("`time`", Type.LONG, 100L, 200L).end().end()
        .end()
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, ORCSchemaUtil.convert(schema));
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testTimezoneSensitiveTypes() {
    TimeZone currentTz = TimeZone.getDefault();
    try {
      for (String timezone : new String[]{"America/New_York", "Asia/Kolkata", "UTC/Greenwich"}) {
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
        OffsetDateTime tsTzPredicate = OffsetDateTime.parse("2019-10-02T00:47:28.207366Z");
        OffsetDateTime tsPredicate = OffsetDateTime.parse("1968-01-16T13:07:59.048625Z");
        OffsetDateTime epoch = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

        Schema schema = new Schema(
            required(1, "date", Types.DateType.get()),
            required(2, "tsTz", Types.TimestampType.withZone()),
            required(3, "ts", Types.TimestampType.withoutZone())
        );

        Expression expr = and(
            and(equal("date", 10L), equal("tsTz", ChronoUnit.MICROS.between(epoch, tsTzPredicate))),
            equal("ts", ChronoUnit.MICROS.between(epoch, tsPredicate))
        );
        Expression boundFilter = Binder.bind(schema.asStruct(), expr, true);
        SearchArgument expected = SearchArgumentFactory.newBuilder()
            .startAnd()
            .equals("`date`", Type.DATE, Date.valueOf(LocalDate.parse("1970-01-11", DateTimeFormatter.ISO_LOCAL_DATE)))
            // .equals("`tsTz`", Type.TIMESTAMP, Timestamp.from(tsTzPredicate.toInstant()))
            // .equals("`ts`", Type.TIMESTAMP, Timestamp.from(tsPredicate.toInstant()))
            .end()
            .build();

        SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, ORCSchemaUtil.convert(schema));
        Assert.assertEquals(expected.toString(), actual.toString());
      }
    } finally {
      TimeZone.setDefault(currentTz);
    }
  }

  @Test
  public void testUnsupportedTypes() {
    Schema schema = new Schema(
        required(1, "binary", Types.BinaryType.get()),
        required(2, "fixed", Types.FixedType.ofLength(5)),
        required(3, "uuid", Types.UUIDType.get()),
        // use optional fields for performing isNull checks because Iceberg itself resolves them for required fields
        optional(4, "struct", Types.StructType.of(
            required(5, "long", Types.LongType.get())
        )),
        optional(6, "list", Types.ListType.ofRequired(7, Types.LongType.get())),
        optional(8, "map", Types.MapType.ofRequired(9, 10, Types.LongType.get(), Types.LongType.get()))
    );

    // all operations for these types should resolve to YES_NO_NULL
    Expression expr = and(
        and(
          and(equal("binary", ByteBuffer.allocate(10)), notEqual("fixed", ByteBuffer.allocate(5))),
          and(greaterThan("uuid", UUID.fromString("1-2-3-4-5")), isNull("struct"))
        ),
        and(notNull("list"), isNull("map"))
    );
    Expression boundFilter = Binder.bind(schema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .literal(TruthValue.YES_NO_NULL)
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, ORCSchemaUtil.convert(schema));
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testNestedPrimitives() {
    Schema schema = new Schema(
        optional(1, "struct", Types.StructType.of(
            required(2, "long", Types.LongType.get())
        )),
        optional(3, "list", Types.ListType.ofRequired(4, Types.LongType.get())),
        optional(5, "map", Types.MapType.ofRequired(6, 7, Types.LongType.get(), Types.LongType.get())),
        optional(8, "listOfStruct", Types.ListType.ofRequired(9, Types.StructType.of(
            required(10, "long", Types.LongType.get()))))
    );

    Expression expr = and(
        and(equal("struct.long", 1), equal("list.element", 2)),
        and(equal("map.key", 3), equal("listOfStruct.long", 4))
    );
    Expression boundFilter = Binder.bind(schema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("`struct`.`long`", Type.LONG, 1L)
        .equals("`list`.`_elem`", Type.LONG, 2L)
        .equals("`map`.`_key`", Type.LONG, 3L)
        .equals("`listOfStruct`.`_elem`.`long`", Type.LONG, 4L)
        .end()
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, ORCSchemaUtil.convert(schema));
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testSpecialCharacters() {
    Schema schema = new Schema(
        required(1, "col.with.dots", Types.StructType.of(
            required(2, "inner.col.with.dots", Types.LongType.get())
        )),
        required(3, "colW!th$peci@lCh@rs", Types.LongType.get()),
        required(4, "colWith`Quotes`", Types.LongType.get())
    );

    Expression expr = and(
        equal("col.with.dots.inner.col.with.dots", 1),
        and(equal("colW!th$peci@lCh@rs", 2), equal("colWith`Quotes`", 3))
    );
    Expression boundFilter = Binder.bind(schema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("`col.with.dots`.`inner.col.with.dots`", Type.LONG, 1L)
        .equals("`colW!th$peci@lCh@rs`", Type.LONG, 2L)
        .equals("`colWith``Quotes```", Type.LONG, 3L)
        .end()
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, ORCSchemaUtil.convert(schema));
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testEvolvedSchema() {
    Schema fileSchema = new Schema(
        required(1, "int", Types.IntegerType.get()),
        optional(2, "long_to_be_dropped", Types.LongType.get())
    );

    Schema evolvedSchema = new Schema(
        required(1, "int_renamed", Types.IntegerType.get()),
        optional(3, "float_added", Types.FloatType.get())
    );

    TypeDescription readSchema = ORCSchemaUtil.buildOrcProjection(evolvedSchema, ORCSchemaUtil.convert(fileSchema));

    Expression expr = equal("int_renamed", 1);
    Expression boundFilter = Binder.bind(evolvedSchema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .equals("`int`", Type.LONG, 1L)
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, readSchema);
    Assert.assertEquals(expected.toString(), actual.toString());

    // for columns not in the file, buildOrcProjection will append field names with _r<ID>
    // this will be passed down to ORC, but ORC will handle such cases and return a TruthValue during evaluation
    expr = equal("float_added", 1);
    boundFilter = Binder.bind(evolvedSchema.asStruct(), expr, true);
    expected = SearchArgumentFactory.newBuilder()
        .equals("`float_added_r3`", Type.FLOAT, 1.0)
        .build();

    actual = ExpressionToSearchArgument.convert(boundFilter, readSchema);
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testOriginalSchemaNameMapping() {
    Schema originalSchema = new Schema(
        required(1, "int", Types.IntegerType.get()),
        optional(2, "long", Types.LongType.get())
    );

    TypeDescription orcSchemaWithoutIds = ORCSchemaUtil.removeIds(ORCSchemaUtil.convert(originalSchema));
    NameMapping nameMapping = MappingUtil.create(originalSchema);

    TypeDescription readSchema = ORCSchemaUtil.buildOrcProjection(originalSchema,
        ORCSchemaUtil.applyNameMapping(orcSchemaWithoutIds, nameMapping));

    Expression expr = and(equal("int", 1), equal("long", 1));
    Expression boundFilter = Binder.bind(originalSchema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .equals("`int`", Type.LONG, 1L)
        .equals("`long`", Type.LONG, 1L)
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, readSchema);
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testModifiedSimpleSchemaNameMapping() {
    Schema originalSchema = new Schema(
        required(1, "int", Types.IntegerType.get()),
        optional(2, "long_to_be_dropped", Types.LongType.get())
    );
    Schema mappingSchema = new Schema(
        required(1, "int", Types.IntegerType.get()),
        optional(3, "new_float_field", Types.FloatType.get())
    );
    TypeDescription orcSchemaWithoutIds = ORCSchemaUtil.removeIds(ORCSchemaUtil.convert(originalSchema));
    NameMapping nameMapping = MappingUtil.create(mappingSchema);

    TypeDescription readSchema = ORCSchemaUtil.buildOrcProjection(mappingSchema,
        ORCSchemaUtil.applyNameMapping(orcSchemaWithoutIds, nameMapping));

    Expression expr = equal("int", 1);
    Expression boundFilter = Binder.bind(mappingSchema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .equals("`int`", Type.LONG, 1L)
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, readSchema);
    Assert.assertEquals(expected.toString(), actual.toString());

    // for columns not in the file, buildOrcProjection will append field names with _r<ID>
    // this will be passed down to ORC, but ORC will handle such cases and return a TruthValue during evaluation
    expr = equal("new_float_field", 1);
    boundFilter = Binder.bind(mappingSchema.asStruct(), expr, true);
    expected = SearchArgumentFactory.newBuilder()
        .equals("`new_float_field_r3`", Type.FLOAT, 1.0)
        .build();

    actual = ExpressionToSearchArgument.convert(boundFilter, readSchema);
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testModifiedComplexSchemaNameMapping() {
    Schema originalSchema = new Schema(
        optional(1, "struct", Types.StructType.of(
            required(2, "long", Types.LongType.get())
        )),
        optional(3, "list", Types.ListType.ofRequired(4, Types.LongType.get())),
        optional(5, "map", Types.MapType.ofRequired(6, 7, Types.LongType.get(), Types.LongType.get())),
        optional(8, "listOfStruct", Types.ListType.ofRequired(9, Types.StructType.of(
            required(10, "long", Types.LongType.get())))),
        optional(11, "listOfPeople", Types.ListType.ofRequired(12, Types.StructType.of(
            required(13, "name", Types.StringType.get()),
            required(14, "birth_date", Types.DateType.get()))))
    );
    Schema mappingSchema = new Schema(
        optional(1, "struct", Types.StructType.of(
            required(2, "int", Types.LongType.get())
        )),
        optional(3, "list", Types.ListType.ofRequired(4, Types.LongType.get())),
        optional(5, "newMap", Types.MapType.ofRequired(6, 7, Types.StringType.get(), Types.LongType.get())),
        optional(8, "listOfStruct", Types.ListType.ofRequired(9, Types.StructType.of(
            required(10, "newLong", Types.LongType.get())))),
        optional(11, "listOfPeople", Types.ListType.ofRequired(12, Types.StructType.of(
            required(13, "name", Types.StringType.get()),
            required(14, "age", Types.IntegerType.get()))))
    );
    TypeDescription orcSchemaWithoutIds = ORCSchemaUtil.removeIds(ORCSchemaUtil.convert(originalSchema));
    NameMapping nameMapping = MappingUtil.create(mappingSchema);

    TypeDescription readSchema = ORCSchemaUtil.buildOrcProjection(mappingSchema,
        ORCSchemaUtil.applyNameMapping(orcSchemaWithoutIds, nameMapping));

    Expression expr = and(
        and(
            equal("struct.int", 1), and(
                lessThanOrEqual("list.element", 5),
                equal("newMap.key", "country")
            ),
            and(
                equal("listOfStruct.newLong", 100L),
                notEqual("listOfPeople.name", "Bob")
            )

        ),
        lessThan("listOfPeople.age", 30)
    );
    Expression boundFilter = Binder.bind(mappingSchema.asStruct(), expr, true);
    SearchArgument expected = SearchArgumentFactory.newBuilder()
        .startAnd()
        // Drops struct.long
        .equals("`struct`.`int_r2`", Type.LONG, 1L)
        .lessThanEquals("`list`.`_elem`", Type.LONG, 5L)
        // Drops map
        .equals("`newMap_r5`.`_key`", Type.STRING, "country")
        // Drops listOfStruct.long
        .equals("`listOfStruct`.`_elem`.`newLong_r10`", Type.LONG, 100L)
        .startOr()
        .isNull("`listOfPeople`.`_elem`.`name`", Type.STRING)
        .startNot().equals("`listOfPeople`.`_elem`.`name`", Type.STRING, "Bob").end()
        .end()
        .lessThan("`listOfPeople`.`_elem`.`age_r14`", Type.LONG, 30L)
        .end()
        .build();

    SearchArgument actual = ExpressionToSearchArgument.convert(boundFilter, readSchema);
    Assert.assertEquals(expected.toString(), actual.toString());
  }
}
