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

package org.apache.iceberg.flink.source.reader;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.junit.Assert;
import org.junit.Test;

public class TestRecyclableArrayIterator {

  @Test
  public void testEmptyConstruction() {
    // dummy recycler
    final RecyclableArrayIterator<Object> iter = new RecyclableArrayIterator<>(
        ignored -> System.currentTimeMillis());
    Assert.assertNull(iter.next());
  }

  @Test
  public void testGetElements() {
    final String[] elements = new String[]{"1", "2", "3", "4"};
    final long initialOffset = 3;
    final long initialSkipCount = 17;

    // dummy recycler
    final RecyclableArrayIterator<String> iter = new RecyclableArrayIterator<>(
        ignored -> System.currentTimeMillis(), elements, elements.length, initialOffset, initialSkipCount);

    for (int i = 0; i < elements.length; i++) {
      final RecordAndPosition<String> recAndPos = iter.next();
      Assert.assertEquals(elements[i], recAndPos.getRecord());
      Assert.assertEquals(initialOffset, recAndPos.getOffset());
      Assert.assertEquals(initialSkipCount + i + 1, recAndPos.getRecordSkipCount());
    }
  }

  @Test
  public void testExhausted() {
    // dummy recycler
    final RecyclableArrayIterator<String> iter = new RecyclableArrayIterator<>(
        ignored -> System.currentTimeMillis(), new String[]{"1", "2"}, 2, 0L, 0L);

    iter.next();
    iter.next();

    Assert.assertNull(iter.next());
  }

  @Test
  public void testArraySubRange() {
    // dummy recycler
    final RecyclableArrayIterator<String> iter = new RecyclableArrayIterator<>(ignored -> System.currentTimeMillis(),
        new String[]{"1", "2", "3"}, 2, 0L, 0L);

    Assert.assertNotNull(iter.next());
    Assert.assertNotNull(iter.next());
    Assert.assertNull(iter.next());
  }

  @Test
  public void testRecycler() {
    final AtomicBoolean recycled = new AtomicBoolean();
    final RecyclableArrayIterator<String> iter = new RecyclableArrayIterator<>(ignored -> recycled.set(true));
    iter.close();
    Assert.assertTrue(recycled.get());
  }
}
