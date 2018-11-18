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

package org.apache.kylin.storage.druid.read.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.ordering.StringComparator;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.NameMapping;
import org.joda.time.Interval;

import java.text.ParseException;
import java.util.List;
import java.util.Locale;

public class PruneIntervalsProcessor implements Function<FilterCondition, FilterCondition> {
    private final CubeInstance cube;

    private Function<String, Long> formatter;
    // not null only if formatter is not null
    private String partCol;
    private StringComparator partComparator;
    private RangeKey partKey;

    public PruneIntervalsProcessor(CubeInstance cube, NameMapping mapping) {
        this.cube = cube;

        PartitionDesc partitionDesc = cube.getDescriptor().getModel().getPartitionDesc();
        final TblColRef partColRef = partitionDesc.getPartitionDateColumnRef();
        boolean isDatePattern = DateFormat.isDatePattern(partitionDesc.getPartitionDateFormat());

        if (partitionDesc.isPartitioned() && isDatePattern && partitionDesc.getPartitionTimeColumnRef() == null) {
            final FastDateFormat format = DateFormat.getDateFormat(partitionDesc.getPartitionDateFormat());
            DataType dataType = partColRef.getType();
            if (dataType.isDate()) {
                formatter = new Function<String, Long>() {
                    @Override
                    public Long apply(String input) {
                        return Long.parseLong(input);
                    }
                };
            } else if (dataType.isInt() || dataType.isBigInt() || dataType.isStringFamily()) {
                formatter = new Function<String, Long>() {
                    @Override
                    public Long apply(String input) {
                        try {
                            return format.parse(input).getTime();
                        } catch (ParseException e) {
                            String err = String.format(Locale.ROOT,
                                    "Failed to format value %s of partition column %s: %s",
                                    input, partColRef, e.getMessage());
                            throw new RuntimeException(err, e);
                        }
                    }
                };
            }
        }

        if (formatter != null) {
            partCol = mapping.getDimFieldName(partColRef);
            partComparator = DruidSchema.dimensionComparator(partColRef.getType());
            partKey = new RangeKey(partCol, null, partComparator);
        }
    }

    @Override
    public FilterCondition apply(FilterCondition input) {
        final DimFilter filter = input.getDimFilter();
        if (filter == null || partCol == null) {
            return input;
        }

        Result result = extractTimeRanges(filter);
        if (result.isUnable()) {
            return input;
        }

        // prune segments via extracted time ranges
        RangeSet<Long> segmentRanges = TreeRangeSet.create();
        for (CubeSegment segment : cube.getSegments(SegmentStatusEnum.READY)) {
            Range<Long> segmentRange = Range.closedOpen(segment.getTSRange().start.v, segment.getTSRange().start.v);
            boolean containQueryRange = !result.timeRanges.subRangeSet(segmentRange).isEmpty();
            if (segment.getInputRecords() > 0 && containQueryRange) {
                segmentRanges.add(segmentRange);
            }
        }

        List<Interval> intervals = Lists.newArrayList();
        for (Range<Long> range : segmentRanges.asRanges()) {
            intervals.add(new Interval(range.lowerEndpoint(), range.upperEndpoint()));
        }
        return FilterCondition.of(intervals, filter);
    }

    private Result extractTimeRanges(DimFilter filter) {
        if (filter instanceof AndDimFilter) {
            boolean allCompleteConverted = true;
            List<RangeSet<Long>> rangeSets = Lists.newArrayList();

            for (DimFilter child : ((AndDimFilter) filter).getFields()) {
                Result childResult = extractTimeRanges(child);
                allCompleteConverted = allCompleteConverted && childResult.isComplete();
                if (childResult.timeRanges != null) {
                    rangeSets.add(childResult.timeRanges);
                }
            }

            if (rangeSets.isEmpty()) {
                return Result.unable();
            }
            RangeSet<Long> merged = RangeSets.intersect(rangeSets);
            if (allCompleteConverted) {
                return Result.complete(merged);
            } else {
                return Result.partial(merged);
            }

        } else if (filter instanceof OrDimFilter) {
            boolean allCompleteConverted = true;
            List<RangeSet<Long>> rangeSets = Lists.newArrayList();

            for (DimFilter child : ((OrDimFilter) filter).getFields()) {
                Result childResult = extractTimeRanges(child);
                if (childResult.isUnable()) {
                    return Result.unable();
                }

                allCompleteConverted = allCompleteConverted && childResult.isComplete();
                rangeSets.add(childResult.timeRanges);
            }

            RangeSet<Long> merged = RangeSets.union(rangeSets);
            if (allCompleteConverted) {
                return Result.complete(merged);
            } else {
                return Result.partial(merged);
            }

        } else if (filter instanceof NotDimFilter) {
            Result result = extractTimeRanges(((NotDimFilter) filter).getField());
            if (result.isComplete()) {
                return Result.complete(result.timeRanges.complement());
            }
            return Result.unable();

        } else {
            RangeKey key = RangeKey.from(filter, partComparator);
            if (!partKey.equals(key)) {
                return Result.unable();
            }
            RangeSet<RangeValue> rangeSet = RangeSets.from(filter, partComparator);
            try {
                RangeSet<Long> longRangeSet = toLong(rangeSet, formatter);
                return Result.complete(longRangeSet);

            } catch (RuntimeException e) {
                // parsing filter value could fail for int/string type partition column,
                // e.g, "where part_dt = 'abcd'" is a valid sql if part_dt is of string type.
                // in that case, we rely on druid to do the filter work.
                return Result.unable();
            }
        }
    }

    private static RangeSet<Long> toLong(RangeSet<RangeValue> rangeSet, Function<String, Long> formatter) {
        RangeSet<Long> result = TreeRangeSet.create();
        for (Range<RangeValue> range : rangeSet.asRanges()) {
            Range<Long> longRange;
            if (!range.hasLowerBound() && !range.hasUpperBound()) {
                longRange = Range.all();
            } else if (range.hasLowerBound() && !range.hasUpperBound()) {
                Long lower = formatter.apply(range.lowerEndpoint().getValue());
                longRange = Range.downTo(lower, range.lowerBoundType());
            } else if (!range.hasLowerBound() && range.hasUpperBound()) {
                Long upper = formatter.apply(range.upperEndpoint().getValue());
                longRange = Range.upTo(upper, range.upperBoundType());
            } else {
                Long lower = formatter.apply(range.lowerEndpoint().getValue());
                Long upper = formatter.apply(range.upperEndpoint().getValue());
                longRange = Range.range(lower, range.lowerBoundType(), upper, range.upperBoundType());
            }
            result.add(longRange);
        }
        return result;
    }

    private static class Result {
        enum ResultType {
            COMPLETE, // when a filter consists only partition column, like dt between '2017-12-01' and '2017-12-10'
            PARTIAL, // when a filter can be converted to partition column filter AND other filter, like dt='2017-12-01' and city='Beijing'
            UNABLE // when a filter can't be converted to partition column filter AND other filter, like dt='2017-12-01' or city='Beijing'
        }

        RangeSet<Long> timeRanges;
        ResultType type;

        private Result(RangeSet<Long> timeRanges, ResultType type) {
            this.timeRanges = timeRanges;
            this.type = type;
        }

        static Result complete(RangeSet<Long> rangeSet) {
            Preconditions.checkNotNull(rangeSet);
            return new Result(rangeSet, ResultType.COMPLETE);
        }

        static Result partial(RangeSet<Long> rangeSet) {
            Preconditions.checkNotNull(rangeSet);
            return new Result(rangeSet, ResultType.PARTIAL);
        }

        static Result unable() {
            return new Result(null, ResultType.UNABLE);
        }

        public boolean isComplete() {
            return type == ResultType.COMPLETE;
        }

        public boolean isPartial() {
            return type == ResultType.PARTIAL;
        }

        public boolean isUnable() {
            return type == ResultType.UNABLE;
        }
    }
}
