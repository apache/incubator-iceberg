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

package org.apache.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

// inspired in part by https://github.com/apache/avro/blob/release-1.8.2/lang/java/guava/src/main/java/org/apache/avro/GuavaClasses.java
public class GuavaClasses {

  /*
   * Referencing Guava classes here includes them in the minimized and relocated Guava jar
   */
  static {
    VisibleForTesting.class.getName();
    Joiner.class.getName();
    MoreObjects.class.getName();
    Objects.class.getName();
    Preconditions.class.getName();
    Splitter.class.getName();
    Throwables.class.getName();
    BiMap.class.getName();
    FluentIterable.class.getName();
    ImmutableBiMap.class.getName();
    ImmutableList.class.getName();
    ImmutableMap.class.getName();
    ImmutableSet.class.getName();
    Iterables.class.getName();
    Iterators.class.getName();
    ListMultimap.class.getName();
    Lists.class.getName();
    MapMaker.class.getName();
    Maps.class.getName();
    Multimap.class.getName();
    Multimaps.class.getName();
    Ordering.class.getName();
    Sets.class.getName();
    Streams.class.getName();
    Hasher.class.getName();
    HashFunction.class.getName();
    Hashing.class.getName();
    Files.class.getName();
    Bytes.class.getName();
    MoreExecutors.class.getName();
    ThreadFactoryBuilder.class.getName();
    Iterables.class.getName();
    CountingOutputStream.class.getName();
  }
}
