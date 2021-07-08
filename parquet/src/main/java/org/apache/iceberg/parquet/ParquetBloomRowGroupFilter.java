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

package org.apache.iceberg.parquet;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetBloomRowGroupFilter {
  private final Schema schema;
  private final Expression expr;

  public ParquetBloomRowGroupFilter(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public ParquetBloomRowGroupFilter(Schema schema, Expression unbound, boolean caseSensitive) {
    this.schema = schema;
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, Expressions.rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether the bloom for a row group may contain records that match the expression.
   *
   * @param fileSchema  schema for the Parquet file
   * @param bloomReader a bloom filter reader
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean shouldRead(MessageType fileSchema, BlockMetaData rowGroup,
      BloomFilterReader bloomReader) {
    return new BloomEvalVisitor().eval(fileSchema, rowGroup, bloomReader);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class BloomEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private BloomFilterReader bloomReader;
    private Map<Integer, Boolean> isFallback = null;
    private Map<Integer, ColumnDescriptor> cols = null;
    private Map<Integer, ColumnChunkMetaData> columnMetaMap = null;

    private boolean eval(MessageType fileSchema, BlockMetaData rowGroup, BloomFilterReader bloomFilterReader) {
      this.bloomReader = bloomFilterReader;
      this.isFallback = Maps.newHashMap();
      this.cols = Maps.newHashMap();
      this.columnMetaMap = Maps.newHashMap();

      for (ColumnDescriptor desc : fileSchema.getColumns()) {
        PrimitiveType colType = fileSchema.getType(desc.getPath()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          cols.put(id, desc);
        }
      }

      for (ColumnChunkMetaData meta : rowGroup.getColumns()) {
        PrimitiveType colType = fileSchema.getType(meta.getPath().toArray()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          isFallback.put(id, ParquetUtil.hasNonBloomFilterPages(meta));
          columnMetaMap.put(id, meta);
        }
      }

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return ROWS_MIGHT_MATCH; // all rows match
    }

    @Override
    public Boolean alwaysFalse() {
      return ROWS_CANNOT_MATCH; // all rows fail
    }

    @Override
    public Boolean not(Boolean result) {
      // not() should be rewritten by RewriteNot
      // bloom filter is based on hash and cannot eliminate based on not
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(BoundReference<T> ref) {
      // bloom filter only contain non-nulls and cannot eliminate based on isNull or NotNull
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      // bloom filter only contain non-nulls and cannot eliminate based on isNull or NotNull
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean isNaN(BoundReference<T> ref) {
      // bloom filter is based on hash and cannot eliminate based on isNaN or notNaN
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNaN(BoundReference<T> ref) {
      // bloom filter is based on hash and cannot eliminate based on isNaN or notNaN
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonBloomFilter = isFallback.get(id);
      if (hasNonBloomFilter == null || hasNonBloomFilter) {
        return ROWS_MIGHT_MATCH;
      }

      BloomFilter bloom = getBloomById(id);
      T value = lit.value();
      return bloom.findHash(tryHash(id, value, bloom)) ? ROWS_MIGHT_MATCH : ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on notEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      int id = ref.fieldId();

      Boolean hasNonBloomFilter = isFallback.get(id);
      if (hasNonBloomFilter == null || hasNonBloomFilter) {
        return ROWS_MIGHT_MATCH;
      }

      BloomFilter bloom = getBloomById(id);
      for (T e : literalSet) {
        if (bloom.findHash(tryHash(id, e, bloom))) {
          // found hash so rows match
          return ROWS_MIGHT_MATCH;
        }
      }

      // no hash found so rows don't match
      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      // bloom filter is based on hash and cannot eliminate based on notIn
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on startsWith
      return ROWS_MIGHT_MATCH;
    }

    private BloomFilter getBloomById(int id) {
      ColumnChunkMetaData columnChunkMetaData = columnMetaMap.get(id);
      BloomFilter bloomFilter = bloomReader.readBloomFilter(columnChunkMetaData);

      if (bloomFilter == null) {
        throw new IllegalStateException("Failed to read required bloom filter for id: " + id);
      }

      return bloomFilter;
    }

    private <T> long tryHash(int id, T value, BloomFilter bloom) {
      ColumnDescriptor col = cols.get(id);

      switch (col.getPrimitiveType().getPrimitiveTypeName()) {
        case BINARY:
          return bloom.hash(Binary.fromString(value.toString()));
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          return bloom.hash(value);
        default:
          throw new IllegalArgumentException("Cannot hash of type: " + col.getPrimitiveType().getPrimitiveTypeName());
      }
    }
  }
}
