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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.iceberg.deletes.DeleteSetter;
import org.apache.iceberg.io.CloseableIterable;

public abstract class DeleteMarker<T> {

  private final DeleteSetter<T> deleteSetter;

  public DeleteMarker(DeleteSetter<T> deleteSetter) {
    this.deleteSetter = deleteSetter;
  }

  protected abstract boolean shouldDelete(T item);

  private Iterable<T> setDeleted(Iterable<T> iterable) {
    return () -> new InternalIterator(iterable.iterator());
  }

  public CloseableIterable<T> setDeleted(CloseableIterable<T> iterable) {
    return CloseableIterable.combine(setDeleted((Iterable<T>) iterable), iterable);
  }

  private class InternalIterator implements Iterator<T> {
    private final Iterator<T> iterator;

    private InternalIterator(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      T current = iterator.next();

      deleteSetter.wrap(current);
      if (!deleteSetter.isDeleted() && shouldDelete(current)) {
        return deleteSetter.setDeleted();
      } else {
        return current;
      }
    }
  }
}
