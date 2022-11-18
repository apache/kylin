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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TableDescResponse extends TableDesc {
    @JsonProperty("exd")
    private Map<String, String> descExd = new HashMap<>();
    @JsonProperty("root_fact")
    private boolean rootFact;
    @JsonProperty("lookup")
    private boolean lookup;
    @JsonProperty("primary_key")
    private Set<String> primaryKey = new HashSet<>();
    @JsonProperty("foreign_key")
    private Set<String> foreignKey = new HashSet<>();
    @JsonProperty("partitioned_column")
    private String partitionedColumn;
    @JsonProperty("partitioned_column_format")
    private String partitionedColumnFormat;
    @JsonProperty("segment_range")
    private SegmentRange segmentRange;
    @JsonProperty("storage_size")
    private long storageSize = -1;
    @JsonProperty("total_records")
    private long totalRecords;
    @JsonProperty("sampling_rows")
    private List<String[]> samplingRows = new ArrayList<>();
    @JsonProperty("columns")
    private ColumnDescResponse[] extColumns;
    @JsonProperty("last_build_job_id")
    private String jodID;
    @JsonProperty("excluded")
    private boolean excluded;

    @JsonProperty("kafka_bootstrap_servers")
    private String kafkaBootstrapServers;
    @JsonProperty("subscribe")
    private String subscribe;
    @JsonProperty("batch_table_identity")
    private String batchTable;
    @JsonProperty("parser_name")
    private String parserName;

    public TableDescResponse(TableDesc table) {
        super(table);
        extColumns = new ColumnDescResponse[getColumns().length];
        for (int i = 0; i < getColumns().length; i++) {
            extColumns[i] = new ColumnDescResponse(getColumns()[i]);
        }
    }

    @JsonProperty(value = "cardinality", access = JsonProperty.Access.READ_ONLY)
    public Map<String, Long> getCardinality() {
        Map<String, Long> cardinality = Maps.newHashMapWithExpectedSize(extColumns.length);
        for (ColumnDescResponse extColumn : extColumns) {
            if (extColumn.getCardinality() != null) {
                cardinality.put(extColumn.getName(), extColumn.getCardinality());
            }
        }
        return cardinality;
    }

    @JsonProperty(value = "is_transactional", access = JsonProperty.Access.READ_ONLY)
    public boolean getTransactionalV2() {
        return super.isTransactional();
    }

    @Getter
    @Setter
    public static class ColumnDescResponse extends ColumnDesc {
        @JsonProperty("cardinality")
        private Long cardinality;
        @JsonProperty("min_value")
        private String minValue;
        @JsonProperty("max_value")
        private String maxValue;
        @JsonProperty("null_count")
        private Long nullCount;
        @JsonProperty("excluded")
        private boolean excluded;

        ColumnDescResponse(ColumnDesc col) {
            super(col);
        }
    }

}
