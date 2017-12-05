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

import java.util.List;

import org.apache.kylin.storage.druid.DruidSchema;
import org.joda.time.Interval;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.druid.query.filter.DimFilter;

public class FilterCondition {
    public static final FilterCondition MATCH_EVERYTHING = FilterCondition.of(ImmutableList.of(DruidSchema.ETERNITY_INTERVAL), null);
    public static final FilterCondition MATCH_NOTHING = FilterCondition.of(ImmutableList.<Interval> of(), null);

    private final List<Interval> intervals;
    private final DimFilter dimFilter;

    private FilterCondition(List<Interval> intervals, DimFilter dimFilter) {
        Preconditions.checkNotNull(intervals, "intervals is null");
        this.intervals = intervals;
        this.dimFilter = dimFilter;
    }

    public static FilterCondition of(List<Interval> intervals, DimFilter dimFilter) {
        return new FilterCondition(intervals, dimFilter);
    }

    public List<Interval> getIntervals() {
        return intervals;
    }

    public DimFilter getDimFilter() {
        return dimFilter;
    }
}
