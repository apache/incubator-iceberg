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

package org.apache.iceberg.io;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class TestCheckIterator {

  @Test
  public void testIteration() {
    CheckIterator<Integer> checkEvenIterator = new CheckIterator<Integer>(
        CloseableIterable.withNoopClose(IntStream.range(0, 5).boxed().collect(Collectors.toList())).iterator()) {
      @Override
      protected boolean check(Integer item) {
        return (item % 2) == 0;
      }
    };

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(checkEvenIterator.hasNext());
      Optional<Integer> next = checkEvenIterator.next();
      if (i % 2 == 0) {
        Assert.assertTrue(next.isPresent());
      } else {
        Assert.assertFalse(next.isPresent());
      }
    }

    Assert.assertFalse(checkEvenIterator.hasNext());
  }
}
