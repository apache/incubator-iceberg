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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * An Iterator of {@link Optional} that checks a predicate for each item based on another Iterator.
 * If predicate is true, the optional item has content, otherwise the optional is empty.
 *
 * @param <T> the type of objects of the dependent Iterator
 */
public abstract class CheckIterator<T> implements CloseableIterator<Optional<T>> {
  private final Iterator<T> items;
  private boolean closed;

  protected CheckIterator(Iterator<T> items) {
    this.items = items;
    this.closed = false;
  }

  protected abstract boolean check(T item);

  @Override
  public boolean hasNext() {
    boolean itemsHasNext = items.hasNext();
    if (!itemsHasNext) {
      close();
    }

    return !closed && itemsHasNext;
  }

  @Override
  public Optional<T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    T item = items.next();
    return check(item) ? Optional.ofNullable(item) : Optional.empty();
  }

  @Override
  public void close() {
    if (!closed) {
      try {
        ((Closeable) items).close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      this.closed = true;
    }
  }
}
