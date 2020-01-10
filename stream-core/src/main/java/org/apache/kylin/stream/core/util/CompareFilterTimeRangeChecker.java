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

package org.apache.kylin.stream.core.util;

import java.util.Collection;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.TimeDerivedColumnType;
import org.apache.kylin.metadata.filter.CompareTupleFilter;

/**
 * check the time related filter is in a specify time range or not
 */
public class CompareFilterTimeRangeChecker {
    public enum CheckResult {
        INCLUDED, EXCLUDED, OVERLAP;

        public CheckResult or(CheckResult other) {
            if (this == INCLUDED || other == INCLUDED) {
                return INCLUDED;
            }
            if (this == EXCLUDED && other == EXCLUDED) {
                return EXCLUDED;
            }
            return OVERLAP;
        }
    }

    protected long timeStart;
    protected long timeEnd;
    private boolean endClose;

    public CompareFilterTimeRangeChecker(long timeStart, long timeEnd) {
        this(timeStart, timeEnd, false);
    }

    public CompareFilterTimeRangeChecker(long timeStart, long timeEnd, boolean endClose) {
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
        this.endClose = endClose;
    }

    public CheckResult check(CompareTupleFilter compFilter, TimeDerivedColumnType timeDerivedColumnType, long timezoneOffset) {
        Object timestampValue = compFilter.getFirstValue();
        Set conditionValues = compFilter.getValues();
        Pair<Long, Long> timeUnitRange;
        if (timeDerivedColumnType != TimeDerivedColumnType.MINUTE_START
                && timeDerivedColumnType != TimeDerivedColumnType.HOUR_START) {
            timeUnitRange = timezoneOffset == 0 ? timeDerivedColumnType.getTimeUnitRange(timestampValue)
                    : timeDerivedColumnType.getTimeUnitRangeTimezoneAware(timestampValue, timezoneOffset);
        } else {
            timeUnitRange = timeDerivedColumnType.getTimeUnitRange(timestampValue);
        }
        switch (compFilter.getOperator()) {
        case EQ:
            return checkForEqValue(timeUnitRange);
        case NEQ:
            if (timeUnitRange.getFirst() <= timeStart && timeUnitRange.getSecond() >= timeEnd) {
                return CheckResult.EXCLUDED;
            }
            if (timeUnitRange.getSecond() <= timeStart || timeUnitRange.getFirst() >= timeEnd) {
                return CheckResult.INCLUDED;
            }
            return CheckResult.OVERLAP;
        case LT:
            if ((!endClose && timeUnitRange.getFirst() >= timeEnd) || (endClose && timeUnitRange.getFirst() > timeEnd)) {
                return CheckResult.INCLUDED;
            }
            if (timeUnitRange.getFirst() <= timeStart) {
                return CheckResult.EXCLUDED;
            }
            return CheckResult.OVERLAP;
        case LTE:
            if (timeUnitRange.getFirst() >= timeEnd) {
                return CheckResult.INCLUDED;
            }
            if (timeUnitRange.getSecond() < timeStart) {
                return CheckResult.EXCLUDED;
            }
            return CheckResult.OVERLAP;
        case GT:
            if (timeUnitRange.getSecond() < timeStart) {
                return CheckResult.INCLUDED;
            }
            if (timeUnitRange.getFirst() >= timeEnd) {
                return CheckResult.EXCLUDED;
            }
            return CheckResult.OVERLAP;
        case GTE:
            if (timeUnitRange.getFirst() <= timeStart) {
                return CheckResult.INCLUDED;
            }
            if ((!endClose && timeUnitRange.getFirst() >= timeEnd) || (endClose && timeUnitRange.getFirst() > timeEnd)) {
                return CheckResult.EXCLUDED;
            }
            return CheckResult.OVERLAP;
        case IN:
            return checkForInValues(timeDerivedColumnType, conditionValues, timezoneOffset);
        default:
            return CheckResult.OVERLAP;
        }
    }

    private CheckResult checkForEqValue(Pair<Long, Long> timeUnitRange) {
        if (timeUnitRange.getFirst() <= timeStart && timeUnitRange.getSecond() >= timeEnd) {
            return CheckResult.INCLUDED;
        }
        if (timeUnitRange.getSecond() <= timeStart || timeUnitRange.getFirst() >= timeEnd) {
            return CheckResult.EXCLUDED;
        }
        return CheckResult.OVERLAP;
    }

    private CheckResult checkForInValues(TimeDerivedColumnType timeDerivedColumnType, Collection<Object> values, long timezoneOffset) {
        CheckResult result = null;
        for (Object timestampValue : values) {
            Pair<Long, Long> timeUnitRange = timeDerivedColumnType.getTimeUnitRangeTimezoneAware(timestampValue, timezoneOffset);
            CheckResult checkResult = checkForEqValue(timeUnitRange);
            if (result == null) {
                result = checkResult;
            } else {
                result = result.or(checkResult);
            }
        }
        return result;
    }

}
