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

package org.apache.kylin.engine.mr.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.ExecutableContext;

import javax.annotation.Nullable;

public class CubingExecutableUtil {

    public static final String CUBE_NAME = "cubeName";
    public static final String SEGMENT_ID = "segmentId";
    public static final String MERGING_SEGMENT_IDS = "mergingSegmentIds";
    public static final String STATISTICS_PATH = "statisticsPath";
    public static final String CUBING_JOB_ID = "cubingJobId";
    public static final String MERGED_STATISTICS_PATH = "mergedStatisticsPath";
    public static final String INDEX_PATH = "indexPath";

    public static void setStatisticsPath(String path, Map<String, String> params) {
        params.put(STATISTICS_PATH, path);
    }

    public static String getStatisticsPath(Map<String, String> params) {
        return params.get(STATISTICS_PATH);
    }

    public static void setCubeName(String cubeName, Map<String, String> params) {
        params.put(CUBE_NAME, cubeName);
    }

    public static String getCubeName(Map<String, String> params) {
        return params.get(CUBE_NAME);
    }

    public static void setSegmentId(String segmentId, Map<String, String> params) {
        params.put(SEGMENT_ID, segmentId);
    }

    public static String getSegmentId(Map<String, String> params) {
        return params.get(SEGMENT_ID);
    }

    public static void setMergingSegmentIds(List<String> ids, Map<String, String> params) {
        params.put(MERGING_SEGMENT_IDS, StringUtils.join(ids, ","));
    }

    public static CubeSegment findSegment(ExecutableContext context, String cubeName, String segmentId) {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(cubeName);

        if (cube == null) {
            String cubeList = StringUtils.join(Iterables.transform(mgr.listAllCubes(), new Function<CubeInstance, String>() {
                @Nullable
                @Override
                public String apply(@Nullable CubeInstance input) {
                    return input.getName();
                }
            }).iterator(), ",");

            throw new IllegalStateException("target cube name: " + cubeName + " cube list: " + cubeList);
        }

        final CubeSegment newSegment = cube.getSegmentById(segmentId);

        if (newSegment == null) {
            String segmentList = StringUtils.join(Iterables.transform(cube.getSegments(), new Function<CubeSegment, String>() {
                @Nullable
                @Override
                public String apply(@Nullable CubeSegment input) {
                    return input.getUuid();
                }
            }).iterator(), ",");

            throw new IllegalStateException("target segment id: " + segmentId + " segment list: " + segmentList);
        }
        return newSegment;
    }

    public static List<String> getMergingSegmentIds(Map<String, String> params) {
        final String ids = params.get(MERGING_SEGMENT_IDS);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id : splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public static void setCubingJobId(String id, Map<String, String> params) {
        params.put(CUBING_JOB_ID, id);
    }

    public static String getCubingJobId(Map<String, String> params) {
        return params.get(CUBING_JOB_ID);
    }

    public static void setMergedStatisticsPath(String path, Map<String, String> params) {
        params.put(MERGED_STATISTICS_PATH, path);
    }

    public static String getMergedStatisticsPath(Map<String, String> params) {
        return params.get(MERGED_STATISTICS_PATH);
    }

    public static void setIndexPath(String indexPath, Map<String, String> params) {
        params.put(INDEX_PATH, indexPath);
    }

    public static String getIndexPath(Map<String, String> params) {
        return params.get(INDEX_PATH);
    }

}
