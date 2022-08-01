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

package org.apache.kylin.metadata.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@Getter
@Setter
public class QueryHistoryResponse {

    public static final String QUERY_HISTORY_ID = "id";
    public static final String QUERY_HISTORY_INFO = "query_history_info";
    public static final String SQL_TEXT = "sql_text";
    public static final String QUERY_TIME = "query_time";
    public static final String QUERY_DURATION = "duration";
    public static final String QUERY_SERVER = "server";
    public static final String SUBMITTER = "submitter";
    public static final String QUERY_STATUS = "query_status";
    public static final String QUERY_ID = "query_id";
    public static final String TOTAL_SCAN_COUNT = "total_scan_count";
    public static final String TOTAL_SCAN_BYTES = "total_scan_bytes";
    public static final String RESULT_ROW_COUNT = "result_row_count";
    public static final String IS_CACHE_HIT = "cache_hit";
    public static final String IS_INDEX_HIT = "index_hit";
    public static final String ENGINE_TYPE = "engine_type";
    public static final String PROJECT_NAME = "project_name";
    public static final String REALIZATIONS = "realizations";
    public static final String ERROR_TYPE = "error_type";

    //query details
    @JsonProperty(QUERY_ID)
    private String queryId;

    @JsonProperty(QUERY_HISTORY_INFO)
    private QueryHistoryInfoResponse queryHistoryInfo;

    @JsonProperty(SQL_TEXT)
    private String sql;

    @JsonProperty(QUERY_TIME)
    private long queryTime;

    @JsonProperty(QUERY_DURATION)
    private long duration;

    @JsonProperty(QUERY_SERVER)
    private String hostName;

    @JsonProperty(SUBMITTER)
    private String querySubmitter;

    @JsonProperty(IS_INDEX_HIT)
    private boolean indexHit;

    @JsonProperty(QUERY_STATUS)
    private String queryStatus;

    @JsonProperty(RESULT_ROW_COUNT)
    private long resultRowCount;

    @JsonProperty(QUERY_HISTORY_ID)
    private long id;

    @JsonProperty(ENGINE_TYPE)
    private String engineType;

    @JsonProperty(TOTAL_SCAN_COUNT)
    private long totalScanCount;

    @JsonProperty(PROJECT_NAME)
    private String projectName;

    @JsonProperty(REALIZATIONS)
    private List<QueryRealization> nativeQueryRealizations;

    @JsonProperty(TOTAL_SCAN_BYTES)
    private long totalScanBytes;

    @JsonProperty(ERROR_TYPE)
    private String errorType;

    @JsonProperty(IS_CACHE_HIT)
    private boolean cacheHit;

    private String queryRealizations;
}
