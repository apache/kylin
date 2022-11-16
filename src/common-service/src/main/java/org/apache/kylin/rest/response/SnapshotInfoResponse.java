/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.rest.response;

import java.util.Set;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.SnapshotStatus;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.metadata.model.TableExtDesc;

@Getter
@Setter
public class SnapshotInfoResponse implements Comparable<SnapshotInfoResponse> {

    @JsonProperty("table")
    private String table;

    @JsonProperty("database")
    private String database;

    @JsonProperty("usage")
    private int usage;

    @JsonProperty("total_rows")
    private long totalRows;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("fact_table_count")
    private int factTableCount;

    @JsonProperty("lookup_table_count")
    private int lookupTableCount;

    @JsonProperty("last_modified_time")
    private long lastModifiedTime;

    @JsonProperty("status")
    private SnapshotStatus status;

    @JsonProperty("forbidden_colunms")
    private Set<String> columns;

    @JsonProperty("select_partition_col")
    private String selectPartitionCol;

    @JsonProperty("source_type")
    private int sourceType;

    public SnapshotInfoResponse() {
    }

    public SnapshotInfoResponse(TableDesc tableDesc, TableExtDesc tableExtDesc, long totalRows, int factTableCount, int lookupTableCount,
                                SnapshotStatus status, Set<String> columns) {

        this.table = tableDesc.getName();
        this.database = tableDesc.getDatabase();
        this.usage = tableExtDesc.getSnapshotHitCount() + tableDesc.getSnapshotHitCount();
        this.totalRows = totalRows;
        this.storage = tableDesc.getLastSnapshotSize();
        this.factTableCount = factTableCount;
        this.lookupTableCount = lookupTableCount;
        this.lastModifiedTime = tableDesc.getSnapshotLastModified();
        this.status = status;
        this.columns = columns;
        this.selectPartitionCol = tableDesc.getSelectedSnapshotPartitionCol();
        this.sourceType = tableDesc.getSourceType();
    }

    @Override
    public int compareTo(SnapshotInfoResponse o) {
        if (this.lastModifiedTime == 0) {
            return -1;
        }

        if (o.lastModifiedTime == 0) {
            return 1;
        }

        int nonNegative = o.lastModifiedTime > this.lastModifiedTime ? 1 : 0;
        return o.lastModifiedTime < this.lastModifiedTime ? -1 : nonNegative;
    }
}
