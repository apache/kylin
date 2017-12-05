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

import io.druid.query.ordering.StringComparator;

public class RangeValue implements Comparable<RangeValue> {
    private final String value;
    private final StringComparator comparator;

    public RangeValue(String value, StringComparator comparator) {
        this.value = value;
        this.comparator = comparator;
    }

    public String getValue() {
        return value;
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
        RangeValue rangeValue = (RangeValue) o;
        return Objects.equals(value, rangeValue.value) && Objects.equals(comparator, rangeValue.comparator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, comparator);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int compareTo(RangeValue o) {
        return comparator.compare(value, o.value);
    }
}
