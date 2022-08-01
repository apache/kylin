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

package org.apache.kylin.rest.request;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.acl.SensitiveDataMask;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class AclTCRRequest {
    @JsonProperty("database_name")
    private String databaseName;

    @JsonProperty
    private List<Table> tables;

    @Data
    public static class Table {
        @JsonProperty("table_name")
        private String tableName;

        @JsonProperty
        private boolean authorized;

        @JsonProperty
        private List<Column> columns;

        // Default value for rows, like_rows and row_filter is null
        // DO NOT CHANGE TO EMPTY LIST OR EMPTY ROW FILTER
        @JsonProperty
        private List<Row> rows;

        @JsonProperty("like_rows")
        private List<Row> likeRows;

        @JsonProperty("row_filter")
        private RowFilter rowFilter;
    }

    @Data
    public static class Column {
        @JsonProperty("column_name")
        private String columnName;

        @JsonProperty
        private boolean authorized;

        @JsonProperty("data_mask_type")
        private SensitiveDataMask.MaskType dataMaskType;

        @JsonProperty("dependent_columns")
        private List<DependentColumnData> dependentColumns;
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
        private boolean group;

        @JsonProperty
        private List<Filter> filters = new ArrayList<>();
    }

    @Data
    public static class RowFilter {
        @JsonProperty
        private String type = "AND";

        @JsonProperty("filter_groups")
        private List<FilterGroup> filterGroups = new ArrayList<>();
    }

    @Data
    public static class DependentColumnData {
        @JsonProperty("column_identity")
        private String columnIdentity;

        @JsonProperty("values")
        private String[] values;
    }
}
