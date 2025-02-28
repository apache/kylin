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

package org.apache.kylin.metadata.acl;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@EqualsAndHashCode
public class AclTCR extends RootPersistentEntity {

    //wrap read only aclTCR

    private String resourceName;
    private boolean principal;

    public void init(String resourceName, String project, boolean principal) {
        this.resourceName = resourceName;
        this.project = project;
        this.principal = principal;
    }

    @JsonProperty
    private Table table = null;

    @Override
    public String resourceName() {
        return resourceName;
    }

    public static String generateResourceName(String project, String sid, boolean principal) {
        return project + "." + (principal ? "u" : "g") + "." + sid;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.ACL;
    }

    public boolean isPrincipal() {
        return principal;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public boolean isAuthorized(String dbTblName) {
        if (Objects.isNull(table)) {
            return true;
        }
        return table.containsKey(dbTblName);
    }

    public boolean isAuthorized(String dbTblName, String columnName) {
        if (Objects.isNull(table)) {
            return true;
        }
        if (!table.containsKey(dbTblName)) {
            return false;
        }
        if (Objects.isNull(table.get(dbTblName)) || Objects.isNull(table.get(dbTblName).getColumn())) {
            return true;
        }
        return table.get(dbTblName).getColumn().contains(columnName);
    }

    public boolean isColumnAuthorized(String columnIdentity) {
        int sepIdx = columnIdentity.lastIndexOf('.');
        return isAuthorized(columnIdentity.substring(0, sepIdx), columnIdentity.substring(sepIdx + 1));
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Table extends TreeMap<String, ColumnRow> {

        // # { DB.TABLE1: { "columns": ["COL1","COL2","COL3"], "rows":{COL1:["A","B","C"]} } } #
        public Table() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ColumnRow {

        @JsonProperty
        private Column column = null;

        @JsonProperty
        private Row row = null;

        @JsonProperty("column_sensitive_data_mask")
        private List<SensitiveDataMask> columnSensitiveDataMask = null;

        @JsonProperty("dependent_columns")
        private List<DependentColumn> dependentColumns = null;

        @JsonProperty("like_row")
        private Row likeRow = null;

        @JsonProperty("row_filter")
        private List<FilterGroup> rowFilter = null;

        public void setRowFilter(List<FilterGroup> rowFilter) {
            this.rowFilter = rowFilter;
        }

        public List<FilterGroup> getRowFilter() {
            return rowFilter;
        }

        public void setLikeRow(Row likeRow) {
            this.likeRow = likeRow;
        }

        public Row getLikeRow() {
            return likeRow;
        }

        public Column getColumn() {
            return column;
        }

        public void setColumn(Column column) {
            this.column = column;
        }

        public Row getRow() {
            return row;
        }

        public void setRow(Row row) {
            this.row = row;
        }

        public Map<String, SensitiveDataMask> getColumnSensitiveDataMaskMap() {
            Map<String, SensitiveDataMask> maskMap = new HashMap<>();
            if (columnSensitiveDataMask != null) {
                for (SensitiveDataMask mask : columnSensitiveDataMask) {
                    maskMap.put(mask.getColumn(), mask);
                }
            }
            return maskMap;
        }

        public List<SensitiveDataMask> getColumnSensitiveDataMask() {
            return columnSensitiveDataMask;
        }

        public void setColumnSensitiveDataMask(List<SensitiveDataMask> columnSensitiveDataMask) {
            this.columnSensitiveDataMask = columnSensitiveDataMask;
        }

        public void setDependentColumns(List<DependentColumn> dependentColumns) {
            this.dependentColumns = dependentColumns;
        }

        public List<DependentColumn> getDependentColumns() {
            return dependentColumns;
        }

        public Map<String, Collection<DependentColumn>> getDependentColMap() {
            Map<String, Collection<DependentColumn>> map = new HashMap<>();
            if (dependentColumns != null) {
                for (DependentColumn dependentColumn : dependentColumns) {
                    map.putIfAbsent(dependentColumn.getColumn(), new LinkedList<>());
                    map.get(dependentColumn.getColumn()).add(dependentColumn);
                }
            }
            return map;
        }

        public boolean isAllRowGranted() {
            return MapUtils.isEmpty(row) && MapUtils.isEmpty(likeRow) && CollectionUtils.isEmpty(rowFilter);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Column extends TreeSet<String> {

        // ["COL1", "COL2", "COL3"]
        public Column() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    @Setter
    @Getter
    public static class FilterItems {
        @JsonProperty("in_items")
        private SortedSet<String> inItems = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        @JsonProperty("like_items")
        private SortedSet<String> likeItems = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        @JsonProperty
        private OperatorType type = OperatorType.AND;

        // For deserialization
        public FilterItems() {
        }

        public FilterItems(SortedSet<String> inItems, SortedSet<String> likeItems, OperatorType type) {
            this.inItems = inItems;
            this.likeItems = likeItems;
            this.type = type;
        }

        public static FilterItems merge(FilterItems item1, FilterItems item2) {
            item1.inItems.addAll(item2.inItems);
            item1.likeItems.addAll(item2.likeItems);
            return item1;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Filters extends TreeMap<String, FilterItems> {

        // ["COL1", "COL2", "COL3"]
        public Filters() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    @Setter
    @Getter
    public static class FilterGroup {
        @JsonProperty("is_group")
        private boolean group;

        @JsonProperty
        private OperatorType type = OperatorType.AND;

        @JsonProperty
        private Filters filters = new Filters();
    }

    public enum OperatorType {
        AND, OR;

        private static final Set<String> validValues = Arrays.stream(OperatorType.values()).map(Enum::name)
                .collect(Collectors.toSet());

        private static void validateValue(String value) {
            if (!validValues.contains(value.toUpperCase(Locale.ROOT))) {
                throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "type", "AND or OR");
            }
        }

        public static OperatorType stringToEnum(String value) {
            value = value.toUpperCase(Locale.ROOT);
            validateValue(value);
            return OperatorType.valueOf(value);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Row extends TreeMap<String, RealRow> {

        // # { COL1: [ "A", "B", "C" ] } #
        public Row() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class RealRow extends TreeSet<String> {

        // ["A", "B", "C"]
        public RealRow() {
            super(String.CASE_INSENSITIVE_ORDER);
        }
    }

    /**
     *  One column can have both equal condition row acl and like condition row acl.
     *  E.g. where COUNTRY_NAME in ('China', 'America') or COUNTRY_NAME like 'China%'.
     */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ColumnRealRows {

        @JsonProperty
        private String dbTblColName = null;

        @JsonProperty
        private RealRow realRow = null;

        @JsonProperty
        private RealRow realLikeRow = null;

        public ColumnRealRows() {
        }

        public ColumnRealRows(String dbTblColName, RealRow realRow, RealRow realLikeRow) {
            this.dbTblColName = dbTblColName;
            this.realRow = realRow;
            this.realLikeRow = realLikeRow;
        }

        public RealRow getRealRow() {
            return realRow;
        }

        public RealRow getRealLikeRow() {
            return realLikeRow;
        }
    }
}
