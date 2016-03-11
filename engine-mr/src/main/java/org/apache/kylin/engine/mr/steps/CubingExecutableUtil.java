package org.apache.kylin.engine.mr.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

public class CubingExecutableUtil {

    public static final String CUBE_NAME = "cubeName";
    public static final String SEGMENT_ID = "segmentId";
    public static final String MERGING_SEGMENT_IDS = "mergingSegmentIds";
    public static final String STATISTICS_PATH = "statisticsPath";
    public static final String CUBING_JOB_ID = "cubingJobId";
    public static final String MERGED_STATISTICS_PATH = "mergedStatisticsPath";

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
}
