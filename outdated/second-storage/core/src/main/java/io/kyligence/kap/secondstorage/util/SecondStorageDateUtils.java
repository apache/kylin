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
package io.kyligence.kap.secondstorage.util;

import org.apache.kylin.metadata.model.SegmentRange;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SecondStorageDateUtils {

    public static boolean isInfinite(long start, long end) {
        return start == 0 && end == Long.MAX_VALUE;
    }

    /**
     * @param start Include
     * @param end   exclude
     * @return
     */
    public static List<String> splitByDayStr(long start, long end) {
        return splitByDay(start, end).stream().map(Objects::toString).collect(Collectors.toList());
    }

    public static List<Date> splitByDay(SegmentRange<Long> range) {
        return splitByDay(range.getStart(), range.getEnd());
    }

    public static List<Date> splitByDay(long start, long end) {
        if (end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("segmentRange end is invalid.");
        }
        List<Date> partitions = new ArrayList<>();
        while (start < end) {
            partitions.add(new Date(start));
            start = start + 24 * 60 * 60 * 1000;
        }
        return partitions;
    }
}
