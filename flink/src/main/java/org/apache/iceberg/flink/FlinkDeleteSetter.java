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

package org.apache.iceberg.flink;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.iceberg.deletes.DeleteSetter;

public class FlinkDeleteSetter extends DeleteSetter<RowData> {

  public FlinkDeleteSetter() {
  }

  private int deleteColumnIndex() {
    return 0;
  }

  @Override
  public boolean isDeleted() {
    return row().getBoolean(deleteColumnIndex());
  }

  @Override
  public RowData setDeleted() {
    RowData rowData = this.row();

    int idx = deleteColumnIndex();
    if (rowData instanceof GenericRowData) {
      ((GenericRowData) rowData).setField(idx, true);
    } else if (rowData instanceof UpdatableRowData) {
      ((UpdatableRowData) rowData).setField(idx, true);
    } else {
      throw new UnsupportedOperationException(
          rowData.getClass() + " does not support set the _deleted column to be true.");
    }

    return rowData;
  }
}
