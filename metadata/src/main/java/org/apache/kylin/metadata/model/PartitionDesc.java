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

package org.apache.kylin.metadata.model;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.StringSplitter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class PartitionDesc {

    public static enum PartitionType {
        APPEND, //
        UPDATE_INSERT // not used since 0.7.1
    }

    @JsonProperty("partition_date_column")
    private String partitionDateColumn;
    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;
    @JsonProperty("partition_type")
    private PartitionType partitionType = PartitionType.APPEND;
    @JsonProperty("partition_condition_builder")
    private String partitionConditionBuilderClz = DefaultPartitionConditionBuilder.class.getName();

    private TblColRef partitionDateColumnRef;
    private IPartitionConditionBuilder partitionConditionBuilder;

    public void init(Map<String, TableDesc> tables) {
        if (StringUtils.isEmpty(partitionDateColumn))
            return;

        partitionDateColumn = partitionDateColumn.toUpperCase();

        String[] columns = StringSplitter.split(partitionDateColumn, ".");

        if (null != columns && columns.length == 3) {
            String tableName = columns[0].toUpperCase() + "." + columns[1].toUpperCase();

            TableDesc table = tables.get(tableName);
            ColumnDesc col = table.findColumnByName(columns[2]);
            if (col != null) {
                partitionDateColumnRef = new TblColRef(col);
            } else {
                throw new IllegalStateException("The column '" + partitionDateColumn + "' provided in 'partition_date_column' doesn't exist.");
            }
        } else {
            throw new IllegalStateException("The 'partition_date_column' format is invalid: " + partitionDateColumn + ", it should be {db}.{table}.{column}.");
        }

        partitionConditionBuilder = (IPartitionConditionBuilder) ClassUtil.newInstance(partitionConditionBuilderClz);
    }

    public boolean isPartitioned() {
        return partitionDateColumnRef != null;
    }

    public String getPartitionDateColumn() {
        return partitionDateColumn;
    }

    public void setPartitionDateColumn(String partitionDateColumn) {
        this.partitionDateColumn = partitionDateColumn;
    }

    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public PartitionType getCubePartitionType() {
        return partitionType;
    }

    public IPartitionConditionBuilder getPartitionConditionBuilder() {
        return partitionConditionBuilder;
    }

    public void setCubePartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public TblColRef getPartitionDateColumnRef() {
        return partitionDateColumnRef;
    }

    // ============================================================================

    public static interface IPartitionConditionBuilder {
        String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive, Map<String, String> tableAlias);
    }

    public static class DefaultPartitionConditionBuilder implements IPartitionConditionBuilder {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive, Map<String, String> tableAlias) {
            String partitionColumnName = partDesc.getPartitionDateColumn();

            // convert to use table alias
            int indexOfDot = partitionColumnName.lastIndexOf(".");
            if (indexOfDot > 0) {
                String partitionTableName = partitionColumnName.substring(0, indexOfDot);
                if (tableAlias != null && tableAlias.containsKey(partitionTableName))
                    partitionColumnName = tableAlias.get(partitionTableName) + partitionColumnName.substring(indexOfDot);
            }

            StringBuilder builder = new StringBuilder();

            if (startInclusive > 0) {
                builder.append(partitionColumnName + " >= '" + DateFormat.formatToDateStr(startInclusive) + "' ");
                builder.append("AND ");
            }
            builder.append(partitionColumnName + " < '" + DateFormat.formatToDateStr(endExclusive) + "'");

            return builder.toString();
        }

    }

    /**
     * Another implementation of IPartitionConditionBuilder, for the fact tables which have three partition columns "YEAR", "MONTH", and "DAY"; This
     * class will concat the three columns into yyyy-MM-dd format for query hive;
     */
    public static class YearMonthDayPartitionConditionBuilder implements PartitionDesc.IPartitionConditionBuilder {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive, Map<String, String> tableAlias) {

            String partitionColumnName = partDesc.getPartitionDateColumn();
            String partitionTableName;

            // convert to use table alias
            int indexOfDot = partitionColumnName.lastIndexOf(".");
            if (indexOfDot > 0) {
                partitionTableName = partitionColumnName.substring(0, indexOfDot).toUpperCase();
            } else {
                throw new IllegalStateException("The partitionColumnName is invalid: " + partitionColumnName);
            }

            if (tableAlias.containsKey(partitionTableName)) {
                partitionTableName = tableAlias.get(partitionTableName);
            }

            String concatField = String.format("CONCAT(%s.YEAR,'-',%s.MONTH,'-',%s.DAY)", partitionTableName, partitionTableName, partitionTableName);
            StringBuilder builder = new StringBuilder();

            if (startInclusive > 0) {
                builder.append(concatField + " >= '" + DateFormat.formatToDateStr(startInclusive) + "' ");
                builder.append("AND ");
            }
            builder.append(concatField + " < '" + DateFormat.formatToDateStr(endExclusive) + "'");

            return builder.toString();
        }
    }
}
