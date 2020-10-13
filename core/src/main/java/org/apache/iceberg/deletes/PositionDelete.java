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

package org.apache.iceberg.deletes;

import org.apache.iceberg.StructLike;

public class PositionDelete<R> implements StructLike {
  static <T> PositionDelete<T> create() {
    return new PositionDelete<>();
  }

  private CharSequence path;
  private long pos;
  private R row;

  public PositionDelete<R> set(CharSequence newPath, long newPos, R newRow) {
    this.path = newPath;
    this.pos = newPos;
    this.row = newRow;
    return this;
  }

  @Override
  public int size() {
    return 3;
  }

  public CharSequence path() {
    return path;
  }

  public long pos() {
    return pos;
  }

  public R row() {
    return row;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int colPos, Class<T> javaClass) {
    switch (colPos) {
      case 0:
        return (T) path;
      case 1:
        return (T) (Long) pos;
      case 2:
        return (T) row;
      default:
        throw new IllegalArgumentException("No column at position " + colPos);
    }
  }

  @Override
  public <T> void set(int colPos, T value) {
    switch (colPos) {
      case 0:
        this.path = (CharSequence) value;
        break;
      case 1:
        this.pos = (Long) value;
        break;
      case 2:
        this.row = (R) value;
        break;
      default:
        throw new IllegalArgumentException("No column at position " + colPos);
    }
  }
}
