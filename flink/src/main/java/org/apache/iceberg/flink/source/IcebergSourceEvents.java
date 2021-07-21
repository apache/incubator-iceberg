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

package org.apache.iceberg.flink.source;

import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;

public class IcebergSourceEvents {

  /**
   * We can remove this class once FLINK-21364 is resolved.
   */
  public static final class SplitRequestEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final Collection<String> finishedSplitIds;
    @Nullable
    private final String requesterHostname;

    public SplitRequestEvent() {
      this(Collections.emptyList());
    }

    public SplitRequestEvent(Collection<String> finishedSplitIds) {
      this(finishedSplitIds, null);
    }

    public SplitRequestEvent(Collection<String> finishedSplitIds, String requesterHostname) {
      this.finishedSplitIds = finishedSplitIds;
      this.requesterHostname = requesterHostname;
    }

    public Collection<String> finishedSplitIds() {
      return finishedSplitIds;
    }

    public String requesterHostname() {
      return requesterHostname;
    }
  }
}
