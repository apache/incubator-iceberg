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

package org.apache.iceberg.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestStructLikeMap {

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private final String mapType;
  private Map<StructLike, StructLike> map;

  @Parameterized.Parameters(name = "StructLikeType = {0}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {"rocksdb"},
        new Object[] {"in-memory"}
    );
  }

  public TestStructLikeMap(String mapType) {
    this.mapType = mapType;
  }

  @Before
  public void before() {
    Map<String, String> props = Maps.newHashMap();
    props.put(StructLikeMapUtil.IMPL, mapType);

    if (StructLikeMapUtil.ROCKSDB_DIR.equals(mapType)) {
      props.put(StructLikeMapUtil.ROCKSDB_DIR, temp.getRoot().getAbsolutePath());
    }

    this.map = StructLikeMapUtil.load(KEY_TYPE, VAL_TYPE, props);
  }

  @After
  public void after() {
    map.clear();
  }

  private static final Types.StructType KEY_TYPE = Types.StructType.of(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get())
  );

  private static final Types.StructType VAL_TYPE = Types.StructType.of(
      Types.NestedField.optional(1, "value", Types.StringType.get())
  );

  private static GenericRecord key(int id, CharSequence data) {
    return (GenericRecord) GenericRecord.create(KEY_TYPE)
        .copy("id", id, "data", data);
  }

  private static GenericRecord val(CharSequence value) {
    return (GenericRecord) GenericRecord.create(VAL_TYPE)
        .copy("value", value);
  }

  private static void assertKey(StructLike key1, StructLike key2) {
    StructLikeWrapper expected = StructLikeWrapper.forType(KEY_TYPE).set(key1);
    StructLikeWrapper actual = StructLikeWrapper.forType(VAL_TYPE).set(key2);
    Assert.assertEquals(expected, actual);
  }

  private static void assertValue(StructLike val1, StructLike val2) {
    StructLikeWrapper expected = StructLikeWrapper.forType(VAL_TYPE).set(val1);
    StructLikeWrapper actual = StructLikeWrapper.forType(VAL_TYPE).set(val2);
    Assert.assertEquals(expected, actual);
  }

  private static void assertKeySet(Collection<StructLike> expected, Collection<StructLike> actual) {
    StructLikeSet expectedSet = StructLikeSet.create(KEY_TYPE);
    StructLikeSet actualSet = StructLikeSet.create(KEY_TYPE);
    expectedSet.addAll(expected);
    actualSet.addAll(actual);
    Assert.assertEquals(expectedSet, actualSet);
  }

  private static void assertValueSet(Collection<StructLike> expected, Collection<StructLike> actual) {
    StructLikeSet expectedSet = StructLikeSet.create(VAL_TYPE);
    StructLikeSet actualSet = StructLikeSet.create(VAL_TYPE);
    expectedSet.addAll(expected);
    actualSet.addAll(actual);
    Assert.assertEquals(expectedSet, actualSet);
  }

  @Test
  public void testSingleRecord() {
    Assert.assertEquals(0, map.size());

    map.put(key(1, "aaa"), val("1-aaa"));
    Assert.assertEquals(1, map.size());
    Assert.assertFalse(map.isEmpty());
    Assert.assertTrue(map.containsKey(key(1, "aaa")));
    Assert.assertTrue(map.containsValue(val("1-aaa")));
    assertValue(val("1-aaa"), map.get(key(1, "aaa")));

    Set<StructLike> keySet = map.keySet();
    Assert.assertEquals(1, keySet.size());
    Assert.assertTrue(keySet.contains(key(1, "aaa")));

    Collection<StructLike> values = map.values();
    Assert.assertEquals(1, values.size());
    assertValue(val("1-aaa"), values.toArray(new StructLike[0])[0]);

    Set<Map.Entry<StructLike, StructLike>> entrySet = map.entrySet();
    Assert.assertEquals(1, entrySet.size());
    for (Map.Entry<StructLike, StructLike> entry : entrySet) {
      assertKey(key(1, "aaa"), entry.getKey());
      assertValue(val("1-aaa"), entry.getValue());
      break;
    }
  }

  @Test
  public void testMultipleRecord() {
    Record record1 = key(1, "aaa");
    Record record2 = key(2, "bbb");
    Record record3 = key(3, null);

    Assert.assertEquals(0, map.size());

    map.putAll(ImmutableMap.of(record1, val("1-aaa"), record2, val("2-bbb"), record3, val("3-null")));
    Assert.assertEquals(3, map.size());
    Assert.assertTrue(map.containsKey(record1));
    Assert.assertTrue(map.containsKey(record2));
    Assert.assertTrue(map.containsKey(record3));
    Assert.assertTrue(map.containsValue(val("1-aaa")));
    Assert.assertTrue(map.containsValue(val("2-bbb")));
    Assert.assertTrue(map.containsValue(val("3-null")));
    assertValue(val("1-aaa"), map.get(record1));
    assertValue(val("2-bbb"), map.get(record2));
    assertValue(val("3-null"), map.get(record3));

    Set<StructLike> keySet = map.keySet();
    Assert.assertEquals(3, keySet.size());
    assertKeySet(ImmutableSet.of(record1, record2, record3), keySet);

    Collection<StructLike> values = map.values();
    Assert.assertEquals(3, values.size());
    assertValueSet(ImmutableSet.of(val("1-aaa"), val("2-bbb"), val("3-null")), Sets.newHashSet(values));

    Set<Map.Entry<StructLike, StructLike>> entrySet = map.entrySet();
    Assert.assertEquals(3, entrySet.size());
    Set<StructLike> structLikeSet = Sets.newHashSet();
    Set<StructLike> valueSet = Sets.newHashSet();
    for (Map.Entry<StructLike, StructLike> entry : entrySet) {
      structLikeSet.add(entry.getKey());
      valueSet.add(entry.getValue());
    }
    assertKeySet(ImmutableSet.of(record1, record2, record3), structLikeSet);
    assertValueSet(ImmutableSet.of(val("1-aaa"), val("2-bbb"), val("3-null")), valueSet);
  }

  @Test
  public void testRemove() {
    StructLike record = key(1, "aaa");

    map.put(record, val("1-aaa"));
    Assert.assertEquals(1, map.size());
    assertValue(val("1-aaa"), map.get(record));
    assertValue(val("1-aaa"), map.remove(record));
    Assert.assertEquals(0, map.size());

    map.put(record, val("1-aaa"));
    assertValue(val("1-aaa"), map.get(record));
  }
}
