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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.Segments;
import org.apache.spark.sql.Column;
import org.sparkproject.guava.collect.Sets;

public class NSparkCubingUtil {

    public static final String SEPARATOR = "_0_DOT_0_";

    public static final String CC_SEPARATOR = "_0_DOT_CC_0_";

    public static final String SEPARATOR_TMP = "_0_DOT_TMP_0_";

    private NSparkCubingUtil() {
    }

    public static String ids2Str(Set<? extends Number> ids) {
        return String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    public static Set<Long> str2Longs(String str) {
        Set<Long> r = new LinkedHashSet<>();
        for (String id : str.split(",")) {
            r.add(Long.parseLong(id));
        }
        return r;
    }

    public static Set<String> toSegmentIds(Set<NDataSegment> segments) {
        Set<String> r = new LinkedHashSet<>();
        for (NDataSegment seg : segments) {
            r.add(seg.getId());
        }
        return r;
    }

    public static Set<String> toIgnoredTableSet(String tableListStr) {
        if (StringUtils.isBlank(tableListStr)) {
            return Sets.newLinkedHashSet();
        }
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(Arrays.asList(StringSplitter.split(tableListStr, ",")));
        return s;

    }

    public static Set<String> toSegmentIds(Segments<NDataSegment> segments) {
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(segments.stream().map(NDataSegment::getId).collect(Collectors.toList()));
        return s;
    }

    static Set<String> toSegmentIds(String segmentsStr) {
        Set<String> s = Sets.newLinkedHashSet();
        s.addAll(Arrays.asList(segmentsStr.split(",")));
        return s;
    }

    public static Set<Long> toLayoutIds(Set<LayoutEntity> layouts) {
        Set<Long> r = new LinkedHashSet<>();
        for (LayoutEntity layout : layouts) {
            r.add(layout.getId());
        }
        return r;
    }

    static Set<Long> toLayoutIds(String layoutIdStr) {
        Set<Long> s = Sets.newLinkedHashSet();
        s.addAll(Arrays.stream(layoutIdStr.split(",")).map(Long::parseLong).collect(Collectors.toList()));
        return s;
    }

    public static Set<LayoutEntity> toLayouts(IndexPlan indexPlan, Set<Long> layouts) {
        return layouts.stream().map(indexPlan::getLayoutEntity).filter(Objects::nonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SafeVarargs
    public static Set<Integer> combineIndices(Set<Integer>... items) {
        Set<Integer> combined = new LinkedHashSet<>();
        for (Set<Integer> single : items) {
            combined.addAll(single);
        }
        return combined;
    }

    @SafeVarargs
    public static Column[] getColumns(Set<Integer>... items) {
        Set<Integer> indices = combineIndices(items);
        Column[] ret = new Column[indices.size()];
        int index = 0;
        for (Integer i : indices) {
            ret[index] = new Column(String.valueOf(i));
            index++;
        }
        return ret;
    }

    public static String getStoragePath(NDataSegment nDataSegment, Long layoutId, Long bucketId) {
        String hdfsWorkingDir = KapConfig.wrap(nDataSegment.getConfig()).getMetadataWorkingDirectory();
        return hdfsWorkingDir + getStoragePathWithoutPrefix(nDataSegment.getProject(),
                nDataSegment.getDataflow().getId(), nDataSegment.getId(), layoutId, bucketId);
    }

    public static String getStoragePath(NDataSegment nDataSegment, Long layoutId) {
        return getStoragePath(nDataSegment, layoutId, null);
    }

    public static String getStoragePath(NDataSegment nDataSegment) {
        return getStoragePath(nDataSegment, null, null);
    }

    public static String getStoragePathWithoutPrefix(String project, String dataflowId, String segmentId,
            Long layoutId) {
        return getStoragePathWithoutPrefix(project, dataflowId, segmentId, layoutId, null);
    }

    public static String getStoragePathWithoutPrefix(String project, String dataflowId, String segmentId, Long layoutId,
            Long bucketId) {
        final String parquet = "/parquet/";
        if (layoutId == null) {
            return project + parquet + dataflowId + "/" + segmentId;
        }
        if (bucketId == null) {
            return project + parquet + dataflowId + "/" + segmentId + "/" + layoutId;
        } else {
            return project + parquet + dataflowId + "/" + segmentId + "/" + layoutId + "/" + bucketId;
        }
    }

    private static final Pattern DOT_PATTERN = Pattern.compile("\\b([\\w]+)\\.([\\w]+)\\b");

    /**
     * matche any non-whitespace character and blank space
     */
    private static final Pattern BACK_TICK_DOT_PATTERN = Pattern
            .compile("`[^\\f\\n\\r\\t\\v]+?`\\.`[^\\f\\n\\r\\t\\v]+?`");

    private static final Pattern UDF_FUNCTION_PATTERN = Pattern.compile("\\b([\\w`]+)\\.([\\w`]+)\\b([\\(]+)");

    private static final Pattern LETTER_PATTERN = Pattern.compile(".*[a-zA-Z]+.*");

    private static final Pattern FLOATING_POINT = Pattern.compile("\\b[0-9]+.[0-9]*E[0-9]+\\b");

    private static final char QUOTE = '\'';

    private static final String BACK_TICK = Quoting.BACK_TICK.string;

    public static String convertFromDot(String withDot) {
        int literalBegin = withDot.indexOf(QUOTE);
        if (literalBegin != -1) {
            int literalEnd = withDot.indexOf(QUOTE, literalBegin + 1);
            if (literalEnd != -1) {
                return doConvertFromDot(withDot.substring(0, literalBegin), false)
                        + withDot.substring(literalBegin, literalEnd + 1)
                        + convertFromDot(withDot.substring(literalEnd + 1));
            }
        }
        return doConvertFromDot(withDot, false);
    }

    public static String convertFromDotWithBackTick(String withDot) {
        int literalBegin = withDot.indexOf(QUOTE);
        if (literalBegin != -1) {
            int literalEnd = withDot.indexOf(QUOTE, literalBegin + 1);
            if (literalEnd != -1) {
                return doConvertFromDot(withDot.substring(0, literalBegin), true)
                        + withDot.substring(literalBegin, literalEnd + 1)
                        + convertFromDotWithBackTick(withDot.substring(literalEnd + 1));
            }
        }
        return doConvertFromDot(withDot, true);
    }

    private static String doConvertFromDot(String withDot, boolean addBackTick) {
        if (withDot.contains(BACK_TICK)) {
            return convertBackTickExpr(withDot, addBackTick);
        } else {
            return convertWithoutBackTickExpr(withDot, addBackTick);
        }
    }

    private static String convertBackTickExpr(String withDot, boolean addBackTick) {
        String convertResult = withDot;
        Matcher m = BACK_TICK_DOT_PATTERN.matcher(withDot);
        while (m.find()) {
            String target = m.group();
            target = target.replace(BACK_TICK + "." + BACK_TICK, SEPARATOR);
            if (!addBackTick) {
                target = target.replace(BACK_TICK, "");
            }
            convertResult = convertResult.replace(m.group(), target);
        }
        return convertResult;
    }

    private static String convertWithoutBackTickExpr(String withDot, boolean addBackTick) {
        String withoutDot = doConvertComputedColumnFromDot(withDot);
        Matcher m = DOT_PATTERN.matcher(withoutDot);
        while (m.find()) {
            String matched = m.group();
            if (LETTER_PATTERN.matcher(matched).find() && !isFloatingPointNumber(matched)) {
                if (addBackTick) {
                    withoutDot = m.replaceFirst(BACK_TICK + "$1" + SEPARATOR + "$2" + BACK_TICK);
                } else {
                    withoutDot = m.replaceFirst("$1" + SEPARATOR + "$2");
                }
                m = DOT_PATTERN.matcher(withoutDot);
            } else {
                withoutDot = m.replaceFirst("$1" + SEPARATOR_TMP + "$2");
                m = DOT_PATTERN.matcher(withoutDot);
            }
        }
        withoutDot = withoutDot.replace(SEPARATOR_TMP, ".");
        withoutDot = withoutDot.replace(CC_SEPARATOR, ".");
        if (addBackTick) {
            return withoutDot;
        }
        return withoutDot.replace("`", "");
    }

    private static String doConvertComputedColumnFromDot(String exp) {
        String withoutDot = exp;
        Matcher m = UDF_FUNCTION_PATTERN.matcher(exp);
        while (m.find()) {
            withoutDot = m.replaceFirst("$1" + CC_SEPARATOR + "$2(");
            m = UDF_FUNCTION_PATTERN.matcher(withoutDot);
        }
        return withoutDot;
    }

    public static boolean isFloatingPointNumber(String exp) {
        Matcher matcher = FLOATING_POINT.matcher(exp);
        if (!matcher.find()) {
            return false;
        }
        try {
            Double.parseDouble(matcher.group());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static String convertToDot(String withoutDot) {
        return withoutDot.replace(SEPARATOR, ".");
    }

}
