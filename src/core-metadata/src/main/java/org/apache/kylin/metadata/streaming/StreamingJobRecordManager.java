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

package org.apache.kylin.metadata.streaming;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobRecordManager {

    private final JdbcStreamingJobRecordStore jdbcRawRecStore;

    // CONSTRUCTOR
    public static StreamingJobRecordManager getInstance() {
        return Singletons.getInstance(StreamingJobRecordManager.class);
    }

    private StreamingJobRecordManager() throws Exception {
        this.jdbcRawRecStore = new JdbcStreamingJobRecordStore(KylinConfig.getInstanceFromEnv());
    }

    public int insert(StreamingJobRecord record) {
        return jdbcRawRecStore.insert(record);
    }

    public void dropTable() throws SQLException {
        jdbcRawRecStore.dropTable();
    }

    public void deleteStreamingJobRecord() {
        jdbcRawRecStore.deleteStreamingJobRecord(-1L);
    }

    public void deleteStreamingJobRecord(long startTime) {
        jdbcRawRecStore.deleteStreamingJobRecord(startTime);
    }

    public void deleteIfRetainTimeReached() {
        val retainTime = new Date(System.currentTimeMillis()
                - KylinConfig.getInstanceFromEnv().getStreamingJobStatsSurvivalThreshold() * 24 * 60 * 60 * 1000)
                        .getTime();
        jdbcRawRecStore.deleteStreamingJobRecord(retainTime);
    }

    public List<StreamingJobRecord> queryByJobId(String jobId) {
        return jdbcRawRecStore.queryByJobId(jobId);
    }

    public StreamingJobRecord getLatestOneByJobId(String jobId) {
        return jdbcRawRecStore.getLatestOneByJobId(jobId);
    }
}
