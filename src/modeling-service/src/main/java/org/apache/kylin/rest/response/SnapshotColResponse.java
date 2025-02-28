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

import java.util.Arrays;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SnapshotColResponse implements Comparable<SnapshotColResponse> {

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonProperty("partition_col")
    private String partitionCol;

    @JsonProperty("partition_col_type")
    private String partitionColType;

    @JsonProperty("other_column_and_type")
    private Map<String, String> otherCol;

    @JsonProperty("source_type")
    private int sourceType;

    public SnapshotColResponse(String database, String table, String partitionCol, String partitionColType,
            Map<String, String> otherCol, int sourceType) {
        this.database = database;
        this.table = table;
        this.partitionCol = partitionCol;
        this.partitionColType = partitionColType;
        this.otherCol = otherCol;
        this.sourceType = sourceType;
    }

    public static SnapshotColResponse from(TableDesc table) {
        if (table.getPartitionColumn() != null) {

            ColumnDesc partCol = table.findColumnByName(table.getPartitionColumn());
            return new SnapshotColResponse(table.getDatabase(), table.getName(), partCol.getName(),
                    partCol.getDatatype(), excludePartCol(table.getColumns(), partCol.getName()),
                    table.getSourceType());
        } else {
            return new SnapshotColResponse(table.getDatabase(), table.getName(), null, null,
                    excludePartCol(table.getColumns(), null), table.getSourceType());
        }
    }

    public static SnapshotColResponse from(TableDesc table,
            UnaryOperator<SnapshotColResponse> transform) {
        SnapshotColResponse res = from(table);
        return transform != null ? transform.apply(res) : res;
    }

    private static Map<String, String> excludePartCol(ColumnDesc[] columns, String partitionCol) {
        return Arrays.asList(columns).stream().filter(col -> !col.getName().equals(partitionCol))
                .collect(Collectors.toMap(ColumnDesc::getName, ColumnDesc::getDatatype));

    }

    @Override
    public int compareTo(SnapshotColResponse other) {
        if (this.database.compareTo(other.database) != 0) {
            return this.database.compareTo(other.database);
        }
        return this.table.compareTo(other.table);
    }
}
