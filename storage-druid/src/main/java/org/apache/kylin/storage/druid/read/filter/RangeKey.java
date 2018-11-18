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

import java.util.Objects;

import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparator;

public class RangeKey {
    private final String dimension;
    private final ExtractionFn extractionFn;
    private final StringComparator comparator;

    public RangeKey(String dimension, ExtractionFn extractionFn, StringComparator comparator) {
        this.dimension = dimension;
        this.extractionFn = extractionFn;
        this.comparator = comparator;
    }

    public static RangeKey from(DimFilter filter, StringComparator comparator) {
        if (filter instanceof SelectorDimFilter) {
            SelectorDimFilter select = (SelectorDimFilter) filter;
            return new RangeKey(select.getDimension(), select.getExtractionFn(), comparator);
        }
        if (filter instanceof BoundDimFilter) {
            BoundDimFilter bound = (BoundDimFilter) filter;
            return new RangeKey(bound.getDimension(), bound.getExtractionFn(), comparator);
        }
        if (filter instanceof InDimFilter) {
            InDimFilter in = (InDimFilter) filter;
            return new RangeKey(in.getDimension(), in.getExtractionFn(), comparator);
        }
        return null;
    }

    public String getDimension() {
        return dimension;
    }

    public ExtractionFn getExtractionFn() {
        return extractionFn;
    }

    public StringComparator getComparator() {
        return comparator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RangeKey rangeKey = (RangeKey) o;
        return Objects.equals(dimension, rangeKey.dimension) && Objects.equals(extractionFn, rangeKey.extractionFn) && Objects.equals(comparator, rangeKey.comparator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimension, extractionFn, comparator);
    }

    @Override
    public String toString() {
        return "RangeKey{" + "dimension='" + dimension + '\'' + ", extractionFn=" + extractionFn + ", comparator=" + comparator + '}';
    }
}
