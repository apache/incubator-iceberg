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

package org.apache.iceberg.flink.data.vectorized;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.vector.BooleanColumnVector;
import org.apache.flink.table.data.vector.BytesColumnVector;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.DecimalColumnVector;
import org.apache.flink.table.data.vector.DoubleColumnVector;
import org.apache.flink.table.data.vector.FloatColumnVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.table.data.vector.LongColumnVector;
import org.apache.flink.table.data.vector.TimestampColumnVector;

class ConstantColumnVectors {
  private ConstantColumnVectors() {
  }

  static ColumnVector ints(Object constant) {
    return new ConstantIntColumnVector(constant);
  }

  static ColumnVector longs(Object constant) {
    return new ConstantLongColumnVector(constant);
  }

  static ColumnVector booleans(Object constant) {
    return new ConstantBooleanColumnVector(constant);
  }

  static ColumnVector doubles(Object constant) {
    return new ConstantDoubleColumnVector(constant);
  }

  static ColumnVector floats(Object constant) {
    return new ConstantFloatColumnVector(constant);
  }

  static ColumnVector decimals(Object constant) {
    return new ConstantDecimalColumnVector(constant);
  }

  static ColumnVector timestamps(Object constant) {
    return new ConstantTimestampColumnVector(constant);
  }

  static ColumnVector bytes(Object constant) {
    return new ConstantBytesColumnVector(constant);
  }

  private static class ConstantIntColumnVector implements IntColumnVector {

    private final Object constant;

    private ConstantIntColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public int getInt(int i) {
      return (int) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }

  private static class ConstantLongColumnVector implements LongColumnVector {

    private final Object constant;

    private ConstantLongColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public long getLong(int i) {
      return (long) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }

  private static class ConstantBooleanColumnVector implements BooleanColumnVector {
    private final Object constant;

    private ConstantBooleanColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public boolean getBoolean(int i) {
      return (boolean) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }

  private static class ConstantDoubleColumnVector implements DoubleColumnVector {
    private final Object constant;

    private ConstantDoubleColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public double getDouble(int i) {
      return (double) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }

  private static class ConstantFloatColumnVector implements FloatColumnVector {
    private final Object constant;

    private ConstantFloatColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public float getFloat(int i) {
      return (float) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }

  private static class ConstantDecimalColumnVector implements DecimalColumnVector {
    private final Object constant;

    private ConstantDecimalColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
      return (DecimalData) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }

  private static class ConstantTimestampColumnVector implements TimestampColumnVector {
    private final Object constant;

    private ConstantTimestampColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
      return (TimestampData) constant;
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }

  private static class ConstantBytesColumnVector implements BytesColumnVector {
    private final Object constant;

    private ConstantBytesColumnVector(Object constant) {
      this.constant = constant;
    }

    @Override
    public Bytes getBytes(int i) {
      byte[] bytes = null;
      if (constant instanceof byte[]) {
        bytes = (byte[]) constant;
      } else {
        BinaryStringData str = (BinaryStringData) constant;
        bytes = str.toBytes();
      }
      return new Bytes(bytes, 0, bytes.length);
    }

    @Override
    public boolean isNullAt(int i) {
      return constant == null;
    }
  }
}
