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

package org.apache.kylin.cube.gridtable;

import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.AbstractDateDimEnc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ISegment;

public class SegmentGTStartAndEnd {
    private ISegment segment;
    private GTInfo info;

    public SegmentGTStartAndEnd(ISegment segment, GTInfo info) {
        this.segment = segment;
        this.info = info;
    }

    public boolean isUsingDatetimeEncoding(int index) {
        return info.getCodeSystem().getDimEnc(index) instanceof AbstractDateDimEnc;
    }

    public Pair<ByteArray, ByteArray> getSegmentStartAndEnd(int index) {
        ByteArray start;
        if (segment.getDateRangeStart() != Long.MIN_VALUE) {
            start = encodeTime(segment.getDateRangeStart(), index, 1);
        } else {
            start = new ByteArray();
        }

        ByteArray end;
        if (segment.getDateRangeEnd() != Long.MAX_VALUE) {
            end = encodeTime(segment.getDateRangeEnd(), index, -1);
        } else {
            end = new ByteArray();
        }
        return Pair.newPair(start, end);

    }

    private ByteArray encodeTime(long ts, int index, int roundingFlag) {
        String value;
        DataType partitionColType = info.getColumnType(index);
        if (partitionColType.isDate()) {
            value = DateFormat.formatToDateStr(ts);
        } else if (partitionColType.isTimeFamily()) {
            value = DateFormat.formatToTimeWithoutMilliStr(ts);
        } else if (partitionColType.isStringFamily() || partitionColType.isIntegerFamily()) {//integer like 20160101
            String partitionDateFormat = segment.getModel().getPartitionDesc().getPartitionDateFormat();
            if (StringUtils.isEmpty(partitionDateFormat))
                partitionDateFormat = DateFormat.DEFAULT_DATE_PATTERN;
            value = DateFormat.formatToDateStr(ts, partitionDateFormat);
        } else {
            throw new RuntimeException("Type " + partitionColType + " is not valid partition column type");
        }

        ByteBuffer buffer = ByteBuffer.allocate(info.getMaxColumnLength());
        info.getCodeSystem().encodeColumnValue(index, value, roundingFlag, buffer);

        return ByteArray.copyOf(buffer.array(), 0, buffer.position());
    }
}