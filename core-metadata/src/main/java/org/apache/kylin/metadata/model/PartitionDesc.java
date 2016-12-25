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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class PartitionDesc implements Serializable {

    public static enum PartitionType {
        APPEND, //
        UPDATE_INSERT // not used since 0.7.1
    }

    @JsonProperty("partition_date_column")
    private String partitionDateColumn;

    @JsonProperty("partition_time_column")
    private String partitionTimeColumn;

    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;//Deprecated

    @JsonProperty("partition_date_format")
    private String partitionDateFormat = DateFormat.DEFAULT_DATE_PATTERN;

    @JsonProperty("partition_time_format")
    private String partitionTimeFormat = DateFormat.DEFAULT_TIME_PATTERN;

    @JsonProperty("partition_type")
    private PartitionType partitionType = PartitionType.APPEND;

    @JsonProperty("partition_condition_builder")
    private String partitionConditionBuilderClz = DefaultPartitionConditionBuilder.class.getName();

    private TblColRef partitionDateColumnRef;
    private TblColRef partitionTimeColumnRef;
    private IPartitionConditionBuilder partitionConditionBuilder;

    public void init(DataModelDesc model) {
        if (StringUtils.isEmpty(partitionDateColumn))
            return;

        partitionDateColumnRef = model.findColumn(partitionDateColumn);
        partitionDateColumn = partitionDateColumnRef.getIdentity();
        if (StringUtils.isBlank(partitionTimeColumn) == false) {
            partitionTimeColumnRef = model.findColumn(partitionTimeColumn);
            partitionTimeColumn = partitionTimeColumnRef.getIdentity();
        }
        partitionConditionBuilder = (IPartitionConditionBuilder) ClassUtil.newInstance(partitionConditionBuilderClz);
    }

    public boolean partitionColumnIsYmdInt() {
        if (partitionDateColumnRef == null)
            return false;
        
        DataType type = partitionDateColumnRef.getType();
        return (type.isInt() || type.isBigInt()) && DateFormat.isDatePattern(partitionDateFormat);
    }

    public boolean partitionColumnIsTimeMillis() {
        if (partitionDateColumnRef == null)
            return false;
        
        DataType type = partitionDateColumnRef.getType();
        return type.isBigInt() && !DateFormat.isDatePattern(partitionDateFormat);
    }

    public boolean isPartitioned() {
        return partitionDateColumnRef != null;
    }

    public String getPartitionDateColumn() {
        return partitionDateColumn;
    }

    // for test
    public void setPartitionDateColumn(String partitionDateColumn) {
        this.partitionDateColumn = partitionDateColumn;
    }
    
    // for test
    void setPartitionDateColumnRef(TblColRef partitionDateColumnRef) {
        this.partitionDateColumnRef = partitionDateColumnRef;
    }
    
    public String getPartitionTimeColumn() {
        return partitionTimeColumn;
    }

    // for test
    void setPartitionTimeColumn(String partitionTimeColumn) {
        this.partitionTimeColumn = partitionTimeColumn;
    }

    // for test
    void setPartitionTimeColumnRef(TblColRef partitionTimeColumnRef) {
        this.partitionTimeColumnRef = partitionTimeColumnRef;
    }
    
    @Deprecated
    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    @Deprecated
    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public String getPartitionDateFormat() {
        return partitionDateFormat;
    }

    public void setPartitionDateFormat(String partitionDateFormat) {
        this.partitionDateFormat = partitionDateFormat;
    }

    public String getPartitionTimeFormat() {
        return partitionTimeFormat;
    }

    public void setPartitionTimeFormat(String partitionTimeFormat) {
        this.partitionTimeFormat = partitionTimeFormat;
    }

    public PartitionType getCubePartitionType() {
        return partitionType;
    }

    public void setCubePartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public IPartitionConditionBuilder getPartitionConditionBuilder() {
        return partitionConditionBuilder;
    }

    public TblColRef getPartitionDateColumnRef() {
        return partitionDateColumnRef;
    }

    public TblColRef getPartitionTimeColumnRef() {
        return partitionTimeColumnRef;
    }
    
    // ============================================================================

    public static interface IPartitionConditionBuilder {
        String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive);
    }

    public static class DefaultPartitionConditionBuilder implements IPartitionConditionBuilder, java.io.Serializable {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive) {
            StringBuilder builder = new StringBuilder();
            TblColRef partitionDateColumn = partDesc.getPartitionDateColumnRef();
            TblColRef partitionTimeColumn = partDesc.getPartitionTimeColumnRef();

            if (partDesc.partitionColumnIsYmdInt()) {
                buildSingleColumnRangeCondAsYmdInt(builder, partitionDateColumn, startInclusive, endExclusive);
            } else if (partDesc.partitionColumnIsTimeMillis()) {
                buildSingleColumnRangeCondAsTimeMillis(builder, partitionDateColumn, startInclusive, endExclusive);
            } else if (partitionDateColumn != null && partitionTimeColumn == null) {
                buildSingleColumnRangeCondition(builder, partitionDateColumn, startInclusive, endExclusive, partDesc.getPartitionDateFormat());
            } else if (partitionDateColumn == null && partitionTimeColumn != null) {
                buildSingleColumnRangeCondition(builder, partitionTimeColumn, startInclusive, endExclusive, partDesc.getPartitionTimeFormat());
            } else if (partitionDateColumn != null && partitionTimeColumn != null) {
                buildMultipleColumnRangeCondition(builder, partitionDateColumn, partitionTimeColumn, startInclusive, endExclusive, partDesc.getPartitionDateFormat(), partDesc.getPartitionTimeFormat());
            }

            return builder.toString();
        }

        private static void buildSingleColumnRangeCondAsTimeMillis(StringBuilder builder, TblColRef partitionColumn, long startInclusive, long endExclusive) {
            String partitionColumnName = partitionColumn.getTableAlias() + "." + partitionColumn.getName();
            if (startInclusive > 0) {
                builder.append(partitionColumnName + " >= " + startInclusive);
                builder.append(" AND ");
            }
            builder.append(partitionColumnName + " < " + endExclusive);
        }

        private static void buildSingleColumnRangeCondAsYmdInt(StringBuilder builder, TblColRef partitionColumn, long startInclusive, long endExclusive) {
            String partitionColumnName = partitionColumn.getTableAlias() + "." + partitionColumn.getName();
            if (startInclusive > 0) {
                builder.append(partitionColumnName + " >= " + DateFormat.formatToDateStr(startInclusive, DateFormat.COMPACT_DATE_PATTERN));
                builder.append(" AND ");
            }
            builder.append(partitionColumnName + " < " + DateFormat.formatToDateStr(endExclusive, DateFormat.COMPACT_DATE_PATTERN));
        }

        private static void buildSingleColumnRangeCondition(StringBuilder builder, TblColRef partitionColumn, long startInclusive, long endExclusive, String partitionColumnDateFormat) {
            String partitionColumnName = partitionColumn.getTableAlias() + "." + partitionColumn.getName();
            if (startInclusive > 0) {
                builder.append(partitionColumnName + " >= '" + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + "'");
                builder.append(" AND ");
            }
            builder.append(partitionColumnName + " < '" + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + "'");
        }

        private static void buildMultipleColumnRangeCondition(StringBuilder builder, TblColRef partitionDateColumn, TblColRef partitionTimeColumn, long startInclusive, long endExclusive, String partitionColumnDateFormat, String partitionColumnTimeFormat) {
            String partitionDateColumnName = partitionDateColumn.getTableAlias() + "." + partitionDateColumn.getName();
            String partitionTimeColumnName = partitionTimeColumn.getTableAlias() + "." + partitionTimeColumn.getName();
            if (startInclusive > 0) {
                builder.append("(");
                builder.append("(");
                builder.append(partitionDateColumnName + " = '" + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + "'").append(" AND ").append(partitionTimeColumnName + " >= '" + DateFormat.formatToDateStr(startInclusive, partitionColumnTimeFormat) + "'");
                builder.append(")");
                builder.append(" OR ");
                builder.append("(");
                builder.append(partitionDateColumnName + " > '" + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + "'");
                builder.append(")");
                builder.append(")");
                builder.append(" AND ");
            }

            builder.append("(");
            builder.append("(");
            builder.append(partitionDateColumnName + " = '" + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + "'").append(" AND ").append(partitionTimeColumnName + " < '" + DateFormat.formatToDateStr(endExclusive, partitionColumnTimeFormat) + "'");
            builder.append(")");
            builder.append(" OR ");
            builder.append("(");
            builder.append(partitionDateColumnName + " < '" + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + "'");
            builder.append(")");
            builder.append(")");
        }
    }

    /**
     * Another implementation of IPartitionConditionBuilder, for the fact tables which have three partition columns "YEAR", "MONTH", and "DAY"; This
     * class will concat the three columns into yyyy-MM-dd format for query hive;
     */
    public static class YearMonthDayPartitionConditionBuilder implements PartitionDesc.IPartitionConditionBuilder {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, long startInclusive, long endExclusive) {

            TblColRef partitionColumn = partDesc.getPartitionDateColumnRef();
            String tableAlias = partitionColumn.getTableAlias();

            String concatField = String.format("CONCAT(%s.YEAR,'-',%s.MONTH,'-',%s.DAY)", tableAlias, tableAlias, tableAlias);
            StringBuilder builder = new StringBuilder();

            if (startInclusive > 0) {
                builder.append(concatField + " >= '" + DateFormat.formatToDateStr(startInclusive) + "' ");
                builder.append("AND ");
            }
            builder.append(concatField + " < '" + DateFormat.formatToDateStr(endExclusive) + "'");

            return builder.toString();
        }
    }

    public static PartitionDesc getCopyOf(PartitionDesc orig) {
        PartitionDesc ret = new PartitionDesc();
        ret.partitionDateColumn = orig.partitionDateColumn;
        ret.partitionTimeColumn = orig.partitionTimeColumn;
        ret.partitionDateStart = orig.partitionDateStart; //Deprecated
        ret.partitionDateFormat = orig.partitionDateFormat;
        ret.partitionTimeFormat = orig.partitionTimeFormat;
        ret.partitionType = orig.partitionType;
        ret.partitionConditionBuilderClz = orig.partitionConditionBuilderClz;
        return ret;
    }

}
