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

import java.io.Serializable;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class PartitionDesc implements Serializable {

    public static enum PartitionType implements Serializable {
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

    public boolean partitionTimeColumnIsInt() {
        if (partitionTimeColumnRef == null)
            return false;

        DataType type = partitionTimeColumnRef.getType();
        return (type.isInt() || type.isBigInt());
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
    public void setPartitionTimeColumn(String partitionTimeColumn) {
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

    public String getPartitionConditionBuilderClz() {
        return partitionConditionBuilderClz;
    }

    public void setPartitionConditionBuilderClz(String partitionConditionBuilderClz) {
        this.partitionConditionBuilderClz = partitionConditionBuilderClz;
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
        String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange);
    }

    public static class DefaultPartitionConditionBuilder implements IPartitionConditionBuilder, Serializable {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange) {
            long startInclusive = (Long) segRange.start.v;
            long endExclusive = (Long) segRange.end.v;

            if (endExclusive <= startInclusive) {
                return "1=0";
            }

            TblColRef partitionDateColumn = partDesc.getPartitionDateColumnRef();
            TblColRef partitionTimeColumn = partDesc.getPartitionTimeColumnRef();

            StringBuilder builder = new StringBuilder();

            if (partitionDateColumn != null && partitionTimeColumn != null) {
                buildMultipleColumnRangeCondition(builder, partitionDateColumn, partitionTimeColumn, startInclusive, endExclusive, partDesc);
            } else if (partitionDateColumn != null) {
                buildSingleColumnRangeCondition(builder, partitionDateColumn, startInclusive, endExclusive, partDesc, true);
            } else if (partitionTimeColumn != null) {
                buildSingleColumnRangeCondition(builder, partitionTimeColumn, startInclusive, endExclusive, partDesc, false);
            }

            return builder.toString();
        }

        private static void buildSingleColumnRangeCondition(StringBuilder builder, TblColRef partitionColumn,
                long startInclusive, long endExclusive, PartitionDesc partitionDesc, boolean isPartitionDateColumn) {
            String partitionColumnName = partitionColumn.getIdentity();

            String startInc = null;
            String endInc = null;

            if (isPartitionDateColumn) {
                startInc = convertDateConditionValue(startInclusive, partitionDesc);
                endInc = convertDateConditionValue(endExclusive, partitionDesc);
            } else {
                startInc = convertTimeConditionValue(startInclusive, partitionDesc);
                endInc = convertTimeConditionValue(endExclusive, partitionDesc);
            }

            builder.append(partitionColumnName + " >= " + startInc);
            builder.append(" AND ");
            builder.append(partitionColumnName + " < " + endInc);
        }

        private static String convertDateConditionValue(long date, PartitionDesc partitionDesc) {
            if(partitionDesc.partitionColumnIsYmdInt()) {
                return DateFormat.formatToDateStr(date, partitionDesc.getPartitionDateFormat());
            } else if (partitionDesc.partitionColumnIsTimeMillis()) {
                return String.valueOf(date);
            } else  {
                return "'" + DateFormat.formatToDateStr(date, partitionDesc.getPartitionDateFormat()) + "'";
            }
        }

        private static String convertTimeConditionValue(long time, PartitionDesc partitionDesc) {
            //currently supported time format: HH:mm:ss、HH:mm、HH(String/int)
            //TODO: HHmmss、HHmm(String/int)

            if (partitionDesc.partitionTimeColumnIsInt()) {
                return DateFormat.formatToDateStr(time, partitionDesc.getPartitionTimeFormat());
            } else {
                return "'" + DateFormat.formatToDateStr(time, partitionDesc.getPartitionTimeFormat()) + "'";
            }
        }

        private static void buildMultipleColumnRangeCondition(StringBuilder builder, TblColRef partitionDateColumn,
                TblColRef partitionTimeColumn, long startInclusive, long endExclusive, PartitionDesc partitionDesc) {
            String partitionDateColumnName = partitionDateColumn.getIdentity();
            String partitionTimeColumnName = partitionTimeColumn.getIdentity();

            String conditionDateStartValue = convertDateConditionValue(startInclusive, partitionDesc);
            String conditionDateEndValue = convertDateConditionValue(endExclusive, partitionDesc);

            String conditionTimeStartValue = convertTimeConditionValue(startInclusive, partitionDesc);
            String conditionTimeEndValue = convertTimeConditionValue(endExclusive, partitionDesc);

            builder.append("(");
            builder.append("(");
            builder.append(partitionDateColumnName + " = " + conditionDateStartValue).append(" AND ")
                    .append(partitionTimeColumnName + " >= " + conditionTimeStartValue);
            builder.append(")");
            builder.append(" OR ");
            builder.append("(");
            builder.append(partitionDateColumnName + " > " + conditionDateStartValue);
            builder.append(")");
            builder.append(")");

            builder.append(" AND ");

            builder.append("(");
            builder.append("(");
            builder.append(partitionDateColumnName + " = " + conditionDateEndValue).append(" AND ")
                    .append(partitionTimeColumnName + " < " + conditionTimeEndValue);
            builder.append(")");
            builder.append(" OR ");
            builder.append("(");
            builder.append(partitionDateColumnName + " < " + conditionDateEndValue);
            builder.append(")");
            builder.append(")");
        }
    }

    /**
     * Another implementation of IPartitionConditionBuilder, for the fact tables which have three partition columns "YEAR", "MONTH", and "DAY"; This
     * class will concat the three columns into yyyy-MM-dd format for query hive;
     */
    public static class YearMonthDayPartitionConditionBuilder implements IPartitionConditionBuilder {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange) {
            long startInclusive = (Long) segRange.start.v;
            long endExclusive = (Long) segRange.end.v;

            TblColRef partitionColumn = partDesc.getPartitionDateColumnRef();
            String tableAlias = partitionColumn.getTableAlias();

            String concatField = String.format(Locale.ROOT, "CONCAT(%s.YEAR,'-',%s.MONTH,'-',%s.DAY)", tableAlias,
                    tableAlias, tableAlias);
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
