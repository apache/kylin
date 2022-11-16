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

package io.kyligence.kap.secondstorage;

import java.util.Set;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableSet;

public class SecondStorageConstants {
    public static final String P_OLD_SEGMENT_IDS = "oldSegmentIds";
    public static final String P_MERGED_SEGMENT_ID = "mergedSegmentId";
    public static final String NODE_REPLICA = "kylin.second-storage.node-replica";

    // config
    public static final String CONFIG_SECOND_STORAGE_CLUSTER = "kylin.second-storage.cluster-config";

    // job
    public static final String STEP_EXPORT_TO_SECOND_STORAGE = "STEP_EXPORT_TO_SECOND_STORAGE";
    public static final String STEP_REFRESH_SECOND_STORAGE = "STEP_REFRESH_SECOND_STORAGE";
    public static final String STEP_MERGE_SECOND_STORAGE = "STEP_MERGE_SECOND_STORAGE";

    public static final String STEP_SECOND_STORAGE_NODE_CLEAN = "STEP_SECOND_STORAGE_NODE_CLEAN";
    public static final String STEP_SECOND_STORAGE_MODEL_CLEAN = "STEP_SECOND_STORAGE_MODEL_CLEAN";
    public static final String STEP_SECOND_STORAGE_SEGMENT_CLEAN = "STEP_SECOND_STORAGE_SEGMENT_CLEAN";
    public static final String STEP_SECOND_STORAGE_INDEX_CLEAN = "STEP_SECOND_STORAGE_INDEX_CLEAN";
    public static final String STEP_SECOND_STORAGE_REFRESH_SECONDARY_INDEX = "STEP_SECOND_STORAGE_REFRESH_SECONDARY_INDEX";

    public static final Set<String> SKIP_STEP_RUNNING = ImmutableSet.of(STEP_EXPORT_TO_SECOND_STORAGE,
            STEP_REFRESH_SECOND_STORAGE, STEP_MERGE_SECOND_STORAGE, STEP_SECOND_STORAGE_NODE_CLEAN,
            STEP_SECOND_STORAGE_MODEL_CLEAN, STEP_SECOND_STORAGE_INDEX_CLEAN,
            STEP_SECOND_STORAGE_REFRESH_SECONDARY_INDEX);
    public static final Set<String> SKIP_JOB_RUNNING = ImmutableSet.of(STEP_SECOND_STORAGE_SEGMENT_CLEAN);

    // internal config
    public static final String PROJECT_MODEL_SEGMENT_PARAM = "projectModelSegmentParam";
    public static final String PROJECT = "project";



    private SecondStorageConstants() {
    }
}
