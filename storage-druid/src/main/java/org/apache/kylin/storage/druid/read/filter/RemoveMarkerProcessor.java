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
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;

public class RemoveMarkerProcessor implements Function<FilterCondition, FilterCondition> {
    public static final RemoveMarkerProcessor SINGLETON = new RemoveMarkerProcessor();

    private RemoveMarkerProcessor() {}

    @Override
    public FilterCondition apply(FilterCondition condition) {
        DimFilter filter = condition.getDimFilter();
        if (filter == MoreDimFilters.ALWAYS_TRUE) {
            return FilterCondition.MATCH_EVERYTHING;
        }
        if (filter == MoreDimFilters.ALWAYS_FALSE) {
            return FilterCondition.MATCH_NOTHING;
        }
        validateNoMarkerFilter(filter);
        return condition;
    }

    private void validateNoMarkerFilter(DimFilter filter) {
        if (filter instanceof AndDimFilter) {
            for (DimFilter child : ((AndDimFilter) filter).getFields()) {
                validateNoMarkerFilter(child);
            }
        } else if (filter instanceof OrDimFilter) {
            for (DimFilter child : ((OrDimFilter) filter).getFields()) {
                validateNoMarkerFilter(child);
            }
        } else if (filter instanceof NotDimFilter) {
            validateNoMarkerFilter(((NotDimFilter) filter).getField());
        } else if (filter == MoreDimFilters.ALWAYS_TRUE || filter == MoreDimFilters.ALWAYS_FALSE) {
            throw new IllegalStateException("still have mark filter");
        }
    }
}
