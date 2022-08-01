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

package org.apache.kylin.common.debug;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Maps;

/**
 * BackdoorToggles and QueryContext are similar because they're both hosting per-query thread local variables.
 * The difference is that BackdoorToggles are specified by user input and work for debug purpose. QueryContext
 * is used voluntarily by program itself
 *
 * BackdoorToggles is part of SQLRequest, QueryContext does not belong to SQLRequest
 */
public class BackdoorToggles {

    private static final ThreadLocal<Map<String, String>> _backdoorToggles = new ThreadLocal<Map<String, String>>();

    public static void setToggles(Map<String, String> toggles) {
        _backdoorToggles.set(toggles);
    }

    public static void addToggle(String key, String value) {
        Map<String, String> map = _backdoorToggles.get();
        if (map == null) {
            setToggles(Maps.newHashMap());
        }
        _backdoorToggles.get().put(key, value);
    }

    public static void addToggles(Map<String, String> toggles) {
        Map<String, String> map = _backdoorToggles.get();
        if (map == null) {
            setToggles(Maps.newHashMap());
        }
        _backdoorToggles.get().putAll(toggles);
    }

    // try avoid using this generic method
    public static String getToggle(String key) {
        Map<String, String> map = _backdoorToggles.get();
        if (map == null)
            return null;

        return map.get(key);
    }

    public static boolean getDisableCache() {
        return getBoolean(DEBUG_TOGGLE_DISABLE_QUERY_CACHE);
    }

    public static boolean getDisableFuzzyKey() {
        return getBoolean(DEBUG_TOGGLE_DISABLE_FUZZY_KEY);
    }

    public static boolean getIsQueryFromAutoModeling() {
        return getBoolean(QUERY_FROM_AUTO_MODELING);
    }

    public static boolean getIsQueryNonEquiJoinModelEnabled() {
        return getBoolean(QUERY_NON_EQUI_JOIN_MODEL_ENABLED);
    }

    public static String getPartitionDumpDir() {
        return getString(DEBUG_TOGGLE_PARTITION_DUMP_DIR);
    }

    public static String getDumpedPartitionDir() {
        return getString(DEBUG_TOGGLE_DUMPED_PARTITION_DIR);
    }

    public static boolean getCheckAllModels() {
        return getBoolean(DEBUG_TOGGLE_CHECK_ALL_MODELS);
    }

    public static boolean getDisabledRawQueryLastHacker() {
        return getBoolean(DISABLE_RAW_QUERY_HACKER);
    }

    public static int getQueryTimeout() {
        String v = getString(DEBUG_TOGGLE_QUERY_TIMEOUT);
        if (v == null)
            return -1;
        else
            return Integer.parseInt(v);
    }

    public static Pair<Short, Short> getShardAssignment() {
        String v = getString(DEBUG_TOGGLE_SHARD_ASSIGNMENT);
        if (v == null) {
            return null;
        } else {
            String[] parts = StringUtils.split(v, "#");
            return Pair.newPair(Short.parseShort(parts[0]), Short.parseShort(parts[1]));
        }
    }

    public static Integer getStatementMaxRows() {
        String v = getString(ATTR_STATEMENT_MAX_ROWS);
        return v == null ? null : Integer.parseInt(v);
    }

    public static boolean getPrepareOnly() {
        return getBoolean(DEBUG_TOGGLE_PREPARE_ONLY);
    }

    private static String getString(String key) {
        Map<String, String> toggles = _backdoorToggles.get();
        if (toggles == null) {
            return null;
        } else {
            return toggles.get(key);
        }
    }

    private static boolean getBoolean(String key) {
        return "true".equals(getString(key));
    }

    public static void cleanToggles() {
        _backdoorToggles.remove();
    }

    /**
     * set DEBUG_TOGGLE_DISABLE_FUZZY_KEY=true to disable fuzzy key for debug/profile usage
     *
     *
     *
     example:(put it into request body)
     "backdoorToggles": {
        "DEBUG_TOGGLE_DISABLE_FUZZY_KEY": "true"
     }
     */
    public final static String DEBUG_TOGGLE_DISABLE_FUZZY_KEY = "DEBUG_TOGGLE_DISABLE_FUZZY_KEY";

    /**
     * set DEBUG_TOGGLE_DISABLE_QUERY_CACHE=true to prevent using cache for current query
     *
     *
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DEBUG_TOGGLE_DISABLE_QUERY_CACHE": "true"
     }
     */
    public final static String DEBUG_TOGGLE_DISABLE_QUERY_CACHE = "DEBUG_TOGGLE_DISABLE_QUERY_CACHE";

    /**
     * set DEBUG_TOGGLE_QUERY_TIMEOUT="timeout_millis" to overwrite the global timeout settings
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DEBUG_TOGGLE_QUERY_TIMEOUT": "120000"
     }
     */
    public final static String DEBUG_TOGGLE_QUERY_TIMEOUT = "DEBUG_TOGGLE_QUERY_TIMEOUT";

    /**
     * set DEBUG_TOGGLE_SHARD_ASSIGNMENT="totalAssignedWorkers#assignedWorkerID" to specify subset of shards to deal with
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DEBUG_TOGGLE_SHARD_ASSIGNMENT": "4#0"
     }
     */
    public final static String DEBUG_TOGGLE_SHARD_ASSIGNMENT = "DEBUG_TOGGLE_SHARD_ASSIGNMENT";

    /**
     * set DEBUG_TOGGLE_PARTITION_DUMP_DIR="dir" to dump the partitions from storage.
     * The dumped partitions are used for performance profiling, for example.
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DEBUG_TOGGLE_PARTITION_DUMP_DIR": "/tmp/dumping"
     }
     */
    public final static String DEBUG_TOGGLE_PARTITION_DUMP_DIR = "DEBUG_TOGGLE_PARTITION_DUMP_DIR";

    /**
     * set DEBUG_TOGGLE_DUMPED_PARTITION_DIR="dir" to specify the dir to retrieve previously dumped partitions
     * it's a companion toggle with DEBUG_TOGGLE_PARTITION_DUMP_DIR
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DEBUG_TOGGLE_DUMPED_PARTITION_DIR": "/tmp/dumped"
     }
     */
    public final static String DEBUG_TOGGLE_DUMPED_PARTITION_DIR = "DEBUG_TOGGLE_DUMPED_PARTITION_DIR";

    /**
     * set DEBUG_TOGGLE_PREPARE_ONLY="true" to prepare the sql statement and get its result set metadata
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DEBUG_TOGGLE_PREPARE_ONLY": "true"
     }
     */
    public final static String DEBUG_TOGGLE_PREPARE_ONLY = "DEBUG_TOGGLE_PREPARE_ONLY";

    // properties on statement may go with this "channel" too
    /**
     * set ATTR_STATEMENT_MAX_ROWS="maxRows" to statement's max rows property
     *
     example:(put it into request body)
     "backdoorToggles": {
     "ATTR_STATEMENT_MAX_ROWS": "10"
     }
     */
    public final static String ATTR_STATEMENT_MAX_ROWS = "ATTR_STATEMENT_MAX_ROWS";

    /**
     * set DEBUG_TOGGLE_CHECK_ALL_MODELS="true" to check all OlapContexts when selecting realization
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DEBUG_TOGGLE_CHECK_ALL_MODELS": "true"
     }
     */
    public final static String DEBUG_TOGGLE_CHECK_ALL_MODELS = "DEBUG_TOGGLE_CHECK_ALL_MODELS";

    /**
     * set DISABLE_RAW_QUERY_HACKER="true" to disable RawQueryLastHacker.hackNoAggregations()
     *
     example:(put it into request body)
     "backdoorToggles": {
     "DISABLE_RAW_QUERY_HACKER": "true"
     }
     */
    public final static String DISABLE_RAW_QUERY_HACKER = "DISABLE_RAW_QUERY_HACKER";

    public final static String QUERY_FROM_AUTO_MODELING = "QUERY_FROM_AUTO_MODELING";

    public final static String QUERY_NON_EQUI_JOIN_MODEL_ENABLED = "QUERY_NON_EQUI_JOIN_MODEL_ENABLED";
}
