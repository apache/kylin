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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.acl.DependentColumn;
import org.apache.kylin.metadata.acl.SensitiveDataMask;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class AclTCRResponse {

    @JsonProperty("authorized_table_num")
    private int authorizedTableNum;

    @JsonProperty("total_table_num")
    private int totalTableNum;

    @JsonProperty("database_name")
    private String databaseName;

    @JsonProperty
    private List<Table> tables;

    @Data
    public static class Table {
        @JsonProperty
        private boolean authorized;

        @JsonProperty("table_name")
        private String tableName;

        @JsonProperty("authorized_column_num")
        private int authorizedColumnNum;

        @JsonProperty("total_column_num")
        private int totalColumnNum;

        @JsonProperty
        private List<Column> columns = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty
        private List<Row> rows = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("like_rows")
        private List<Row> likeRows = new ArrayList<>();

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("row_filter")
        private RowFilter rowFilter = new RowFilter();
    }

    @Data
    public static class Column {
        @JsonProperty
        private boolean authorized;

        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("data_mask_type")
        private SensitiveDataMask.MaskType dataMaskType;

        @JsonProperty("dependent_columns")
        private List<DependentColumnData> dependentColumns;

        @JsonProperty("datatype")
        private String datatype;

        public void setDependentColumns(Collection<DependentColumn> dependentColumns) {
            this.dependentColumns = dependentColumns.stream()
                    .map(col -> new DependentColumnData(col.getDependentColumnIdentity(), col.getDependentValues()))
                    .collect(Collectors.toList());
        }
    }

    @Data
    public static class Row {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty
        private List<String> items;
    }

    @Data
    public static class Filter {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty("in_items")
        private List<String> inItems = new ArrayList<>();

        @JsonProperty("like_items")
        private List<String> likeItems = new ArrayList<>();
    }

    @Data
    public static class FilterGroup {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("is_group")
        private boolean group = false;

        @JsonProperty
        private List<AclTCRResponse.Filter> filters = new ArrayList<>();
    }

    @Data
    public static class RowFilter {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("filter_groups")
        private List<AclTCRResponse.FilterGroup> filterGroups = new ArrayList<>();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DependentColumnData {
        @JsonProperty("column_identity")
        private String columnIdentity;

        @JsonProperty("values")
        private String[] values;

    }
}
