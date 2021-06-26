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

package org.apache.kylin.engine.spark.job;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.spark.sql.Column;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NSparkCubingUtil {

    static String ids2Str(Set<? extends Number> ids) {
        return String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    static Set<Long> str2Longs(String str) {
        Set<Long> r = new LinkedHashSet<>();
        for (String id : str.split(",")) {
            r.add(Long.parseLong(id));
        }
        return r;
    }

    public static Column[] getColumns(Set<Integer> indices1, Set<Integer> indices2) {
        Set<Integer> ret = new LinkedHashSet<>();
        ret.addAll(indices1);
        ret.addAll(indices2);
        return getColumns(ret);
    }

    public static Column[] getColumns(Collection<Integer> indices) {
        Column[] ret = new Column[indices.size()];
        int index = 0;
        for (Integer i : indices) {
            ret[index] = new Column(String.valueOf(i));
            index++;
        }
        return ret;
    }

    public static Column getFirstColumn(Collection<Integer> indices) {
        return getColumns(indices)[0];
    }

    private static final Pattern DOT_PATTERN = Pattern.compile("(\\S+)\\.(\\D+)");

    public static final String SEPARATOR = "_0_DOT_0_";

    public static String convertFromDot(String withDot) {
        Matcher m = DOT_PATTERN.matcher(withDot);
        String withoutDot = withDot;
        while (m.find()) {
            withoutDot = m.replaceAll("$1" + SEPARATOR + "$2");
            m = DOT_PATTERN.matcher(withoutDot);
        }
        return withoutDot;
    }

    public static String getStoragePath(CubeSegment segment, Long layoutId) {
        return PathManager.getParquetStoragePath(segment.getCubeInstance(), segment.getName(), segment.getStorageLocationIdentifier(), layoutId);
    }

    static Set<String> toSegmentNames(Segments<CubeSegment> segments) {
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(segments.stream().map(CubeSegment::getName).collect(Collectors.toList()));
        return s;
    }

    public static String getStoragePathWithoutPrefix(String project, String cubeId, String segmentId, long layoutId) {
        return  project + "/parquet/" + cubeId + "/" + segmentId + "/" + layoutId;
    }
}
