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

package org.apache.kylin.engine.mr.steps.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class LookupExecutableUtil {

    public static final String CUBE_NAME = "cubeName";
    public static final String LOOKUP_TABLE_NAME = "lookupTableName";
    public static final String PROJECT_NAME = "projectName";
    public static final String LOOKUP_SNAPSHOT_ID = "snapshotID";
    public static final String SEGMENT_IDS = "segments";
    public static final String JOB_ID = "jobID";


    public static void setCubeName(String cubeName, Map<String, String> params) {
        params.put(CUBE_NAME, cubeName);
    }

    public static String getCubeName(Map<String, String> params) {
        return params.get(CUBE_NAME);
    }

    public static void setLookupTableName(String lookupTableName, Map<String, String> params) {
        params.put(LOOKUP_TABLE_NAME, lookupTableName);
    }

    public static String getLookupTableName(Map<String, String> params) {
        return params.get(LOOKUP_TABLE_NAME);
    }

    public static void setProjectName(String projectName, Map<String, String> params) {
        params.put(PROJECT_NAME, projectName);
    }

    public static String getProjectName(Map<String, String> params) {
        return params.get(PROJECT_NAME);
    }

    public static void setLookupSnapshotID(String snapshotID, Map<String, String> params) {
        params.put(LOOKUP_SNAPSHOT_ID, snapshotID);
    }

    public static String getLookupSnapshotID(Map<String, String> params) {
        return params.get(LOOKUP_SNAPSHOT_ID);
    }

    public static List<String> getSegments(Map<String, String> params) {
        final String ids = params.get(SEGMENT_IDS);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            Collections.addAll(result, splitted);
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public static void setSegments(List<String> segments, Map<String, String> params) {
        params.put(SEGMENT_IDS, StringUtils.join(segments, ","));
    }


    public static String getJobID(Map<String, String> params) {
        return params.get(JOB_ID);
    }

    public static void setJobID(String jobID, Map<String, String> params) {
        params.put(JOB_ID, jobID);
    }
    
    public static void updateSnapshotPathToCube(CubeManager cubeManager, CubeInstance cube, String lookupTableName,
            String snapshotPath) throws IOException {
        cubeManager.updateCubeLookupSnapshot(cube, lookupTableName, snapshotPath);
        cube.putSnapshotResPath(lookupTableName, snapshotPath);
    }

    public static void updateSnapshotPathToSegments(CubeManager cubeManager, CubeInstance cube, List<String> segmentIDs, String lookupTableName, String snapshotPath) throws IOException {
        CubeInstance cubeCopy = cube.latestCopyForWrite();
        if (segmentIDs.size() > 0) {
            CubeSegment[] segments = new CubeSegment[segmentIDs.size()];
            for (int i = 0; i < segments.length; i++) {
                CubeSegment segment = cubeCopy.getSegmentById(segmentIDs.get(i));
                if (segment == null) {
                    throw new IllegalStateException("the segment not exist in cube:" + segmentIDs.get(i));
                }
                segment.putSnapshotResPath(lookupTableName, snapshotPath);
                segments[i] = segment;
            }
            CubeUpdate cubeUpdate = new CubeUpdate(cubeCopy);
            cubeUpdate.setToUpdateSegs(segments);
            cubeManager.updateCube(cubeUpdate);

            // Update the input cubeSeg after the resource store updated
            for (int i = 0; i < segments.length; i++) {
                CubeSegment segment = cube.getSegmentById(segmentIDs.get(i));
                segment.putSnapshotResPath(lookupTableName, snapshotPath);
            }
        }
    }

}
