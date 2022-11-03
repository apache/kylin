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

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import lombok.val;
import lombok.var;

@RunWith(TimeZoneTestRunner.class)

public class StreamingJobRecordManagerTest extends NLocalFileMetadataTestCase {

    public static final String JOB_ID = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
    public static final String PROJECT_NAME = "streaming_test";

    private StreamingJobRecordManager streamingJobRecordManager;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        streamingJobRecordManager = StreamingJobRecordManager.getInstance();
    }

    @After
    public void destroy() throws Exception {
        cleanupTestMetadata();
        streamingJobRecordManager.dropTable();
    }

    @Test
    public void testTableName() {
        val store = ReflectionUtils.getField(streamingJobRecordManager, "jdbcRawRecStore");
        val tableName = ReflectionUtils.getField(store, "tableName");
        Assert.assertEquals("test_streaming_job_record", tableName);
    }

    @Test
    public void testInsert() {
        streamingJobRecordManager.insert(mockRecord(JOB_ID));
        val recList = streamingJobRecordManager.queryByJobId(JOB_ID);
        Assert.assertNotNull(recList);
        Assert.assertEquals("START", recList.get(0).getAction());
        Assert.assertEquals(PROJECT_NAME, recList.get(0).getProject());
    }

    @Test
    public void testDeleteStreamingJobRecord() {
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b75_build";
        streamingJobRecordManager.insert(mockRecord(jobId));
        val recList = streamingJobRecordManager.queryByJobId(jobId);
        Assert.assertTrue(!CollectionUtils.isEmpty(streamingJobRecordManager.queryByJobId(jobId)));
        streamingJobRecordManager.deleteStreamingJobRecord();
        Assert.assertTrue(CollectionUtils.isEmpty(streamingJobRecordManager.queryByJobId(jobId)));
    }

    @Test
    public void testDeleteIfRetainTimeReached() {
        val config = getTestConfig();
        config.setProperty("kylin.streaming.jobstats.survival-time-threshold", "1d");
        val jobId = "f6ca1ce7-43fc-4c42-a057-1e95dfb75d93_build";
        streamingJobRecordManager.insert(mockRecord(jobId));
        Assert.assertEquals(1, streamingJobRecordManager.queryByJobId(jobId).size());
        streamingJobRecordManager.deleteIfRetainTimeReached();
        Assert.assertEquals(1, streamingJobRecordManager.queryByJobId(jobId).size());

        val oldRec = mockRecord(jobId);
        oldRec.setCreateTime(System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000);
        streamingJobRecordManager.insert(oldRec);
        Assert.assertEquals(2, streamingJobRecordManager.queryByJobId(jobId).size());

        streamingJobRecordManager.deleteIfRetainTimeReached();
        Assert.assertEquals(1, streamingJobRecordManager.queryByJobId(jobId).size());
    }

    @Test
    public void testDropTable() {
        try {
            val jdbcRawRecStore = (JdbcStreamingJobRecordStore) ReflectionUtils.getField(streamingJobRecordManager,
                    "jdbcRawRecStore");
            val dataSource = (DataSource) ReflectionUtils.getField(jdbcRawRecStore, "dataSource");
            try (Connection conn = dataSource.getConnection()) {
                Assert.assertTrue(JdbcUtil.isTableExists(conn, jdbcRawRecStore.tableName));
            }
            streamingJobRecordManager.dropTable();
            try (Connection conn = dataSource.getConnection()) {
                Assert.assertFalse(JdbcUtil.isTableExists(conn, jdbcRawRecStore.tableName));
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testQueryByJobId() {
        var list = streamingJobRecordManager.queryByJobId(JOB_ID);
        Assert.assertTrue(list.isEmpty());
        streamingJobRecordManager.insert(mockRecord(JOB_ID));
        list = streamingJobRecordManager.queryByJobId(JOB_ID);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals("START", list.get(0).getAction());
        Assert.assertEquals(PROJECT_NAME, list.get(0).getProject());
        Assert.assertNotNull(list.get(0).getCreateTime());

        val rec2 = mockRecord(JOB_ID);
        rec2.setAction("STOP");
        streamingJobRecordManager.insert(rec2);
        list = streamingJobRecordManager.queryByJobId(JOB_ID);
        Assert.assertEquals(2, list.size());
        Assert.assertEquals("STOP", list.get(0).getAction());
        Assert.assertEquals("START", list.get(1).getAction());
    }

    @Test
    public void testGetLatestOneByJobId() {
        var record = streamingJobRecordManager.getLatestOneByJobId(JOB_ID);
        Assert.assertNull(record);
        val first = mockRecord(JOB_ID);
        first.setCreateTime(System.currentTimeMillis() - 1000);
        streamingJobRecordManager.insert(first);
        val second = mockRecord(JOB_ID);
        second.setAction("STOP");
        streamingJobRecordManager.insert(second);
        record = streamingJobRecordManager.getLatestOneByJobId(JOB_ID);
        Assert.assertEquals("STOP", record.getAction());
        Assert.assertEquals(PROJECT_NAME, record.getProject());
        Assert.assertNotNull(record.getCreateTime());

    }

    private StreamingJobRecord mockRecord(String jobId) {
        val record = new StreamingJobRecord();
        record.setJobId(jobId);
        record.setAction("START");
        record.setCreateTime(System.currentTimeMillis());
        record.setProject(PROJECT_NAME);
        return record;
    }

}
