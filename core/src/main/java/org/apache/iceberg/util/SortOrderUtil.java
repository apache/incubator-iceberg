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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.transforms.SortOrderVisitor;

public class SortOrderUtil {

  private SortOrderUtil() {
  }

  public static SortOrder buildSortOrder(Table table) {
    return buildSortOrder(table.schema(), table.spec(), table.sortOrder());
  }

  static SortOrder buildSortOrder(Schema schema, PartitionSpec spec, SortOrder sortOrder) {
    if (sortOrder.isUnsorted() && spec.isUnpartitioned()) {
      return SortOrder.unsorted();
    }

    Multimap<Integer, SortField> sortFieldIndex = Multimaps.index(sortOrder.fields(), SortField::sourceId);

    // build a sort prefix of partition fields that are not already in the sort order
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (PartitionField field : spec.fields()) {
      Collection<SortField> sortFields = sortFieldIndex.get(field.sourceId());
      boolean isSorted = sortFields.stream().anyMatch(sortField ->
          field.transform().equals(sortField.transform()) || sortField.transform().satisfiesOrderOf(field.transform()));
      if (!isSorted) {
        String sourceName = schema.findColumnName(field.sourceId());
        builder.asc(Expressions.transform(sourceName, field.transform()));
      }
    }

    // add the configured sort to the partition spec prefix sort
    SortOrderVisitor.visit(sortOrder, new CopySortOrderFields(builder));

    return builder.build();
  }

  public static Set<String> getSortedColumns(SortOrder sortOrder) {
    if (sortOrder == null) {
      return new HashSet<>();
    } else {
      return sortOrder.fields().stream()
          .map(SortField::sourceId)
          .map(sid -> sortOrder.schema().findColumnName(sid))
          .filter(nf -> nf != null)
          .collect(Collectors.toSet());
    }
  }
}
