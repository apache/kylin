/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.engine.spark.metadata.cube.datatype.DataType;

import java.io.Serializable;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class PartitionDesc implements Serializable {

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof PartitionDesc)) return false;
        final PartitionDesc other = (PartitionDesc) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$partitionDateColumn = this.getPartitionDateColumn();
        final Object other$partitionDateColumn = other.getPartitionDateColumn();
        if (this$partitionDateColumn == null ? other$partitionDateColumn != null : !this$partitionDateColumn.equals(other$partitionDateColumn))
            return false;
        if (this.getPartitionDateStart() != other.getPartitionDateStart()) return false;
        final Object this$partitionDateFormat = this.getPartitionDateFormat();
        final Object other$partitionDateFormat = other.getPartitionDateFormat();
        if (this$partitionDateFormat == null ? other$partitionDateFormat != null : !this$partitionDateFormat.equals(other$partitionDateFormat))
            return false;
        final Object this$partitionType = this.partitionType;
        final Object other$partitionType = other.partitionType;
        if (this$partitionType == null ? other$partitionType != null : !this$partitionType.equals(other$partitionType))
            return false;
        final Object this$partitionConditionBuilderClz = this.getPartitionConditionBuilderClz();
        final Object other$partitionConditionBuilderClz = other.getPartitionConditionBuilderClz();
        if (this$partitionConditionBuilderClz == null ? other$partitionConditionBuilderClz != null : !this$partitionConditionBuilderClz.equals(other$partitionConditionBuilderClz))
            return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof PartitionDesc;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $partitionDateColumn = this.getPartitionDateColumn();
        result = result * PRIME + ($partitionDateColumn == null ? 43 : $partitionDateColumn.hashCode());
        final long $partitionDateStart = this.getPartitionDateStart();
        result = result * PRIME + (int) ($partitionDateStart >>> 32 ^ $partitionDateStart);
        final Object $partitionDateFormat = this.getPartitionDateFormat();
        result = result * PRIME + ($partitionDateFormat == null ? 43 : $partitionDateFormat.hashCode());
        final Object $partitionType = this.partitionType;
        result = result * PRIME + ($partitionType == null ? 43 : $partitionType.hashCode());
        final Object $partitionConditionBuilderClz = this.getPartitionConditionBuilderClz();
        result = result * PRIME + ($partitionConditionBuilderClz == null ? 43 : $partitionConditionBuilderClz.hashCode());
        return result;
    }

    public static enum PartitionType implements Serializable {
        APPEND, //
        UPDATE_INSERT // not used since 0.7.1
    }

    @JsonProperty("partition_date_column")
    private String partitionDateColumn;

    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;//Deprecated

    @JsonProperty("partition_date_format")
    private String partitionDateFormat;

    @JsonProperty("partition_type")
    private PartitionType partitionType = PartitionType.APPEND;

    @JsonProperty("partition_condition_builder")
    private String partitionConditionBuilderClz = DefaultPartitionConditionBuilder.class.getName();

    private TblColRef partitionDateColumnRef;
    private IPartitionConditionBuilder partitionConditionBuilder;

    public void init(DataModel model) {
        if (StringUtils.isEmpty(partitionDateColumn))
            return;

        partitionDateColumnRef = model.findColumn(partitionDateColumn);
        partitionDateColumn = partitionDateColumnRef.getIdentity();
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

    // ============================================================================

    public static interface IPartitionConditionBuilder {
        String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange);
    }

    public static class DefaultPartitionConditionBuilder implements IPartitionConditionBuilder, Serializable {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange) {

            Preconditions.checkState(segRange instanceof SegmentRange.TimePartitionedSegmentRange);
            SegmentRange.TimePartitionedSegmentRange tsr = (SegmentRange.TimePartitionedSegmentRange) segRange;

            long startInclusive = tsr.getStart();
            long endExclusive = tsr.getEnd();

            TblColRef partitionDateColumn = partDesc.getPartitionDateColumnRef();

            StringBuilder builder = new StringBuilder();

            if (partDesc.partitionColumnIsYmdInt()) {
                buildSingleColumnRangeCondAsYmdInt(builder, partitionDateColumn, startInclusive, endExclusive);
            } else if (partDesc.partitionColumnIsTimeMillis()) {
                buildSingleColumnRangeCondAsTimeMillis(builder, partitionDateColumn, startInclusive, endExclusive);
            } else if (partitionDateColumn != null) {
                buildSingleColumnRangeCondition(builder, partitionDateColumn, startInclusive, endExclusive,
                        partDesc.getPartitionDateFormat());
            }
            return builder.toString();
        }

        private static void buildSingleColumnRangeCondAsTimeMillis(StringBuilder builder, TblColRef partitionColumn,
                                                                   long startInclusive, long endExclusive) {
            String partitionColumnName = partitionColumn.getExpressionInSourceDB();
            builder.append(partitionColumnName + " >= " + startInclusive);
            builder.append(" AND ");
            builder.append(partitionColumnName + " < " + endExclusive);
        }

        private static void buildSingleColumnRangeCondAsYmdInt(StringBuilder builder, TblColRef partitionColumn,
                                                               long startInclusive, long endExclusive) {
            String partitionColumnName = partitionColumn.getExpressionInSourceDB();
            builder.append(partitionColumnName + " >= "
                    + DateFormat.formatToDateStr(startInclusive, DateFormat.COMPACT_DATE_PATTERN));
            builder.append(" AND ");
            builder.append(partitionColumnName + " < "
                    + DateFormat.formatToDateStr(endExclusive, DateFormat.COMPACT_DATE_PATTERN));
        }

        private static void buildSingleColumnRangeCondition(StringBuilder builder, TblColRef partitionColumn,
                                                            long startInclusive, long endExclusive, String partitionColumnDateFormat) {
            String partitionColumnName = partitionColumn.getExpressionInSourceDB();

            if (endExclusive <= startInclusive) {
                builder.append("1=1");
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
    }

    public static PartitionDesc getCopyOf(PartitionDesc orig) {
        PartitionDesc ret = new PartitionDesc();
        ret.partitionDateColumn = orig.partitionDateColumn;
        ret.partitionDateStart = orig.partitionDateStart; //Deprecated
        ret.partitionDateFormat = orig.partitionDateFormat;
        ret.partitionType = orig.partitionType;
        ret.partitionConditionBuilderClz = orig.partitionConditionBuilderClz;
        return ret;
    }

}
