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
import java.util.function.Function;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.CheckUtil;
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

        //Support CustomYearMonthDayFieldPartitionConditionBuilder, partitionDateColumn split by ","
        partitionConditionBuilder = (IPartitionConditionBuilder) ClassUtil.newInstance(partitionConditionBuilderClz);
        if (partitionConditionBuilder instanceof CustomYearMonthDayFieldPartitionConditionBuilder) {
            ((CustomYearMonthDayFieldPartitionConditionBuilder)partitionConditionBuilder).init(this, model);
        } else {
            partitionDateColumnRef = model.findColumn(partitionDateColumn);
            partitionDateColumn = partitionDateColumnRef.getIdentity();
            if (StringUtils.isBlank(partitionTimeColumn) == false) {
                partitionTimeColumnRef = model.findColumn(partitionTimeColumn);
                partitionTimeColumn = partitionTimeColumnRef.getIdentity();
            }
        }

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

    public boolean equalsRaw(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionDesc other = (PartitionDesc) obj;

        if (!this.partitionType.equals(other.getCubePartitionType()))
            return false;
        if (!this.partitionConditionBuilderClz.equals(other.partitionConditionBuilderClz))
            return false;
        if (!CheckUtil.equals(this.partitionDateColumn, other.getPartitionDateColumn()))
            return false;
        if (!CheckUtil.equals(this.partitionDateFormat, other.getPartitionDateFormat()))
            return false;
        if (!CheckUtil.equals(this.partitionTimeColumn, other.getPartitionTimeColumn()))
            return false;
        if (!CheckUtil.equals(this.partitionTimeFormat, other.getPartitionTimeFormat()))
            return false;
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionDesc other = (PartitionDesc) obj;

        if (!this.partitionType.equals(other.getCubePartitionType()))
            return false;
        if (!CheckUtil.equals(this.partitionDateColumn, other.getPartitionDateColumn()))
            return false;
        if (!CheckUtil.equals(this.partitionDateFormat, other.getPartitionDateFormat()))
            return false;
        if (this.partitionDateColumn != null) {
            if (!this.partitionConditionBuilder.getClass().equals(other.getPartitionConditionBuilder().getClass()))
                return false;
            if (this.partitionConditionBuilder instanceof DefaultPartitionConditionBuilder) {
                if (!CheckUtil.equals(this.partitionTimeColumn, other.getPartitionTimeColumn())) {
                    return false;
                }
                if (!CheckUtil.equals(this.partitionTimeFormat, other.getPartitionTimeFormat())) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + partitionType.hashCode();
        result = prime * result + partitionConditionBuilderClz.hashCode();
        result = prime * result + ((partitionDateColumn == null) ? 0 : partitionDateColumn.hashCode());
        result = prime * result + ((partitionDateFormat == null) ? 0 : partitionDateFormat.hashCode());
        if (partitionConditionBuilder instanceof DefaultPartitionConditionBuilder) {
            result = prime * result + ((partitionTimeColumn == null) ? 0 : partitionTimeColumn.hashCode());
            result = prime * result + ((partitionTimeFormat == null) ? 0 : partitionTimeFormat.hashCode());
        }
        return result;
    }

    // ============================================================================

    public static interface IPartitionConditionBuilder {
        String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange, Function<TblColRef, String> quoteFunc);
    }

    public static class DefaultPartitionConditionBuilder implements IPartitionConditionBuilder, Serializable {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange, Function<TblColRef, String> quoteFunc) {
            long startInclusive = (Long) segRange.start.v;
            long endExclusive = (Long) segRange.end.v;

            TblColRef partitionDateColumn = partDesc.getPartitionDateColumnRef();
            TblColRef partitionTimeColumn = partDesc.getPartitionTimeColumnRef();

            if (partitionDateColumn != null) {
                partitionDateColumn.setQuotedFunc(quoteFunc);
            }
            if (partitionTimeColumn != null) {
                partitionTimeColumn.setQuotedFunc(quoteFunc);
            }

            StringBuilder builder = new StringBuilder();

            if (partDesc.partitionColumnIsYmdInt()) {
                if (partitionTimeColumn == null) {
                    buildSingleColumnRangeCondAsYmdInt(builder, partitionDateColumn, startInclusive, endExclusive, partDesc.getPartitionDateFormat());
                } else {
                    buildMultipleColumnRangeCondition(builder, partitionDateColumn, partitionTimeColumn, startInclusive,
                            endExclusive, partDesc.getPartitionDateFormat(), partDesc.getPartitionTimeFormat(), true);
                }
            } else if (partDesc.partitionColumnIsTimeMillis()) {
                buildSingleColumnRangeCondAsTimeMillis(builder, partitionDateColumn, startInclusive, endExclusive);
            } else if (partitionDateColumn != null && partitionTimeColumn == null) {
                buildSingleColumnRangeCondition(builder, partitionDateColumn, startInclusive, endExclusive,
                        partDesc.getPartitionDateFormat());
            } else if (partitionDateColumn == null && partitionTimeColumn != null) {
                buildSingleColumnRangeCondition(builder, partitionTimeColumn, startInclusive, endExclusive,
                        partDesc.getPartitionTimeFormat());
            } else if (partitionDateColumn != null && partitionTimeColumn != null) {
                buildMultipleColumnRangeCondition(builder, partitionDateColumn, partitionTimeColumn, startInclusive,
                        endExclusive, partDesc.getPartitionDateFormat(), partDesc.getPartitionTimeFormat(), false);
            }

            return builder.toString();
        }

        private static void buildSingleColumnRangeCondAsTimeMillis(StringBuilder builder, TblColRef partitionColumn,
                                                                   long startInclusive, long endExclusive) {
            String partitionColumnName = partitionColumn.getQuotedIdentity();
            builder.append(partitionColumnName + " >= " + startInclusive);
            builder.append(" AND ");
            builder.append(partitionColumnName + " < " + endExclusive);
        }

        private static void buildSingleColumnRangeCondAsYmdInt(StringBuilder builder, TblColRef partitionColumn,
                                                               long startInclusive, long endExclusive, String partitionColumnDateFormat) {
            String partitionColumnName = partitionColumn.getQuotedIdentity();
            builder.append(partitionColumnName + " >= "
                    + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat));
            builder.append(" AND ");
            builder.append(
                    partitionColumnName + " < " + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat));
        }

        private static void buildSingleColumnRangeCondition(StringBuilder builder, TblColRef partitionColumn,
                                                            long startInclusive, long endExclusive, String partitionColumnDateFormat) {
            String partitionColumnName = partitionColumn.getQuotedIdentity();

            if (endExclusive <= startInclusive) {
                builder.append("1=0");
                return;
            }

            String startInc = null;
            String endInc = null;
            if (StringUtils.isBlank(partitionColumnDateFormat)) {
                startInc = String.valueOf(startInclusive);
                endInc = String.valueOf(endExclusive);
            } else {
                startInc = DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat);
                endInc = DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat);
            }

            builder.append(partitionColumnName + " >= '" + startInc + "'");
            builder.append(" AND ");
            builder.append(partitionColumnName + " < '" + endInc + "'");
        }

        private static void buildMultipleColumnRangeCondition(StringBuilder builder, TblColRef partitionDateColumn,
                                                              TblColRef partitionTimeColumn, long startInclusive, long endExclusive, String partitionColumnDateFormat,
                                                              String partitionColumnTimeFormat, boolean partitionDateColumnIsYmdInt) {
            String partitionDateColumnName = partitionDateColumn.getQuotedIdentity();
            String partitionTimeColumnName = partitionTimeColumn.getQuotedIdentity();
            String singleQuotation = partitionDateColumnIsYmdInt ? "" : "'";
            builder.append("(");
            builder.append("(");
            builder.append(partitionDateColumnName + " = " + singleQuotation
                    + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + singleQuotation).append(" AND ")
                    .append(partitionTimeColumnName + " >= '"
                            + DateFormat.formatToDateStr(startInclusive, partitionColumnTimeFormat) + "'");
            builder.append(")");
            builder.append(" OR ");
            builder.append("(");
            builder.append(partitionDateColumnName + " > " + singleQuotation
                    + DateFormat.formatToDateStr(startInclusive, partitionColumnDateFormat) + singleQuotation);
            builder.append(")");
            builder.append(")");
            builder.append(" AND ");

            builder.append("(");
            builder.append("(");
            builder.append(partitionDateColumnName + " = " + singleQuotation
                    + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + singleQuotation).append(" AND ")
                    .append(partitionTimeColumnName + " < '"
                            + DateFormat.formatToDateStr(endExclusive, partitionColumnTimeFormat) + "'");
            builder.append(")");
            builder.append(" OR ");
            builder.append("(");
            builder.append(partitionDateColumnName + " < " + singleQuotation
                    + DateFormat.formatToDateStr(endExclusive, partitionColumnDateFormat) + singleQuotation);
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
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange, Function<TblColRef, String> func) {
            long startInclusive = (Long) segRange.start.v;
            long endExclusive = (Long) segRange.end.v;

            TblColRef partitionColumn = partDesc.getPartitionDateColumnRef();
            Preconditions.checkNotNull(partitionColumn);
            partitionColumn.setQuotedFunc(func);
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

    /**
     * Another implementation of IPartitionConditionBuilder, for the fact tables which have three custom partition columns like "Y", "M", "D" means "YEAR", "MONTH", and "DAY";
     * This class will conicat the three columns into yyyy-MM-dd format for query hive;
     * implements Serializable for spark build
     */
    public static class CustomYearMonthDayFieldPartitionConditionBuilder implements IPartitionConditionBuilder, Serializable {
        private String yearPartitionDateColumn;
        private String monthPartitionDateColumn;
        private String dayPartitionDateColumn;
        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange, Function<TblColRef, String> func) {
            long startInclusive = (Long) segRange.start.v;
            long endExclusive = (Long) segRange.end.v;

            TblColRef partitionColumn = partDesc.getPartitionDateColumnRef();
            if (partitionColumn != null) {
                partitionColumn.setQuotedFunc(func);
            }
            String concatField = String.format(Locale.ROOT, "CONCAT(%s,'-',%s,'-',%s)", yearPartitionDateColumn,
                    monthPartitionDateColumn, dayPartitionDateColumn);
            StringBuilder builder = new StringBuilder();

            if (startInclusive > 0) {
                builder.append(concatField + " >= '" + DateFormat.formatToDateStr(startInclusive) + "' ");
                builder.append("AND ");
            }
            builder.append(concatField + " < '" + DateFormat.formatToDateStr(endExclusive) + "'");

            return builder.toString();
        }

        public void init(PartitionDesc partitionDesc, DataModelDesc model) {
            String[] yearMonthDayColumns = partitionDesc.getPartitionDateColumn().split(",");
            if (yearMonthDayColumns.length != 3) {
                throw new IllegalArgumentException(partitionDesc.getPartitionDateColumn() + " is not year, month, and day columns");
            }
            TblColRef yearRef = model.findColumn(yearMonthDayColumns[0]);
            yearPartitionDateColumn = yearRef.getIdentity();
            monthPartitionDateColumn = model.findColumn(yearMonthDayColumns[1]).getIdentity();
            dayPartitionDateColumn = model.findColumn(yearMonthDayColumns[2]).getIdentity();
            //for partition desc isPartitioned() true
            partitionDesc.setPartitionDateColumnRef(yearRef);
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
