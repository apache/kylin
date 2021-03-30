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

package org.apache.kylin.stream.core.query;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.util.CompareFilterTimeRangeChecker;
import org.apache.kylin.stream.core.util.CompareFilterTimeRangeChecker.CheckResult;
import org.apache.kylin.dimension.TimeDerivedColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

/**
 * Scan planner for Streaming data segments, take derived time columns into consideration.
 */
public class StreamingDataQueryPlanner {

    private static Logger logger = LoggerFactory.getLogger(StreamingDataQueryPlanner.class);

    protected CubeDesc cubeDesc;
    protected TupleFilter filter;
    protected TupleFilter flatFilter;
    private final long timezoneOffset;

    public StreamingDataQueryPlanner(CubeDesc cubeDesc, TupleFilter filter) {
        this.cubeDesc = cubeDesc;
        this.filter = filter;
        this.flatFilter = flattenToOrAndFilter(filter);
        String timezoneName = cubeDesc.getConfig().getStreamingDerivedTimeTimezone();
        if (timezoneName == null || timezoneName.length() == 0) {
            timezoneOffset = 0;
        } else {
            timezoneOffset = TimeZone.getTimeZone(timezoneName).getRawOffset();
        }
    }

    public boolean canSkip(long timeStart, long timeEnd) {
        return canSkip(timeStart, timeEnd, false);
    }

    public boolean canSkip(long timeStart, long timeEnd, boolean endInclude) {
        if (flatFilter == null) {
            return false;
        }
        CompareFilterTimeRangeChecker timeRangeChecker = new CompareFilterTimeRangeChecker(timeStart, timeEnd, endInclude);
        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != FilterOperatorEnum.AND) {
                throw new IllegalStateException("Filter should be AND instead of " + andFilter);
            }
            if (!canSkipForAndFilter(andFilter, timeRangeChecker)) {
                return false;
            }
        }
        return true;
    }

    private boolean canSkipForAndFilter(TupleFilter andFilter, CompareFilterTimeRangeChecker timeRangeChecker) {
        for (TupleFilter filter : andFilter.getChildren()) {
            if (!(filter instanceof CompareTupleFilter)) {
                if (filter instanceof ConstantTupleFilter && !filter.evaluate(null, null)) {
                    return true;
                } else {
                    continue;
                }
            }
            CompareTupleFilter comp = (CompareTupleFilter) filter;
            TblColRef column = comp.getColumn();
            if (column == null || !TimeDerivedColumnType.isTimeDerivedColumn(column.getName())) {
                continue;
            }
            TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.getTimeDerivedColumnType(column
                    .getName());

            CheckResult checkResult = timeRangeChecker.check(comp, timeDerivedColumnType, timezoneOffset);
            if (checkResult == CheckResult.EXCLUDED) {
                return true;
            } else {
                continue;
            }
        }
        return false;
    }

    protected TupleFilter flattenToOrAndFilter(TupleFilter filter) {
        if (filter == null)
            return null;

        TupleFilter flatFilter = filter.flatFilter();

        // normalize to OR-AND filter
        if (flatFilter.getOperator() == FilterOperatorEnum.AND) {
            LogicalTupleFilter f = new LogicalTupleFilter(FilterOperatorEnum.OR);
            f.addChild(flatFilter);
            flatFilter = f;
        }

        if (flatFilter.getOperator() != FilterOperatorEnum.OR)
            throw new IllegalStateException();

        return flatFilter;
    }
}
