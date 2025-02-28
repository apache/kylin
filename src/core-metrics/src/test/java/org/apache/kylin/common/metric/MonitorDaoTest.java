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
package org.apache.kylin.common.metric;

import static org.mockito.ArgumentMatchers.eq;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.metrics.service.InfluxDBInstance;
import org.apache.kylin.common.metrics.service.JobStatusMonitorMetric;
import org.apache.kylin.common.metrics.service.MonitorDao;
import org.apache.kylin.common.metrics.service.QueryMonitorMetric;
import org.apache.kylin.shaded.influxdb.org.influxdb.dto.QueryResult;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MonitorDaoTest {

    private final InfluxDBInstance influxDBInstance = Mockito.mock(InfluxDBInstance.class);

    @BeforeEach
    public void setUp() {
        Mockito.doReturn("KYLIN_MONITOR").when(influxDBInstance).getDatabase();
        Mockito.doReturn(true).when(influxDBInstance).write(eq("tb_query"), Mockito.anyMap(), Mockito.anyMap(),
                Mockito.anyLong());

        Mockito.doReturn(false).when(influxDBInstance).write(eq("tb_job_status"), Mockito.anyMap(), Mockito.anyMap(),
                Mockito.anyLong());

        Mockito.doReturn(mockQueryMonitorMetricQueryResult()).when(influxDBInstance).read(
                String.format(Locale.ROOT, MonitorDao.QUERY_METRICS_BY_TIME_SQL_FORMAT, "tb_query", 0, Long.MAX_VALUE));
        Mockito.doReturn(mockJobStatusMonitorMetricQueryResult()).when(influxDBInstance).read(String.format(Locale.ROOT,
                MonitorDao.QUERY_METRICS_BY_TIME_SQL_FORMAT, "tb_job_status", 0, Long.MAX_VALUE));
    }

    public QueryMonitorMetric mockQueryMonitorMetric() {
        QueryMonitorMetric monitorMetric = new QueryMonitorMetric();
        monitorMetric.setHost("localhost");
        monitorMetric.setIp("127.0.0.1");
        monitorMetric.setPort("7070");
        monitorMetric.setPid("22333");
        monitorMetric.setNodeType("query");
        monitorMetric.setCreateTime(System.currentTimeMillis());
        monitorMetric.setLastResponseTime(29 * 1000L);

        monitorMetric.setErrorAccumulated(1);
        monitorMetric.setSparkRestarting(false);

        return monitorMetric;
    }

    public QueryResult mockQueryMonitorMetricQueryResult() {
        QueryResult.Series series = new QueryResult.Series();
        series.setName("tb_query");
        series.setColumns(Lists.newArrayList("host", "ip", "port", "pid", "node_type", "create_time", "response_time",
                "error_accumulated", "spark_restarting"));
        List<Object> value = Lists.newArrayList("localhost", "127.0.0.1", "7070", "22333", "query",
                1.0 * System.currentTimeMillis(), 1.0 * 29 * 1000L, 1.0 * 2, Boolean.FALSE);
        series.setValues(Lists.newArrayList());
        series.getValues().add(value);

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Lists.newArrayList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Lists.newArrayList(result));
        return queryResult;
    }

    public JobStatusMonitorMetric mockJobStatusMonitorMetric() {
        JobStatusMonitorMetric monitorMetric = new JobStatusMonitorMetric();
        monitorMetric.setHost("localhost");
        monitorMetric.setIp("127.0.0.1");
        monitorMetric.setPort("7070");
        monitorMetric.setPid("22333");
        monitorMetric.setNodeType("job");
        monitorMetric.setCreateTime(System.currentTimeMillis());

        monitorMetric.setFinishedJobs(1L);
        monitorMetric.setPendingJobs(2L);
        monitorMetric.setErrorJobs(3L);
        return monitorMetric;
    }

    public QueryResult mockJobStatusMonitorMetricQueryResult() {
        QueryResult.Series series = new QueryResult.Series();
        series.setName("tb_job_status");
        series.setColumns(Lists.newArrayList("host", "ip", "port", "pid", "node_type", "create_time", "finished_jobs",
                "pending_jobs", "error_jobs"));
        List<Object> value = Lists.newArrayList("localhost", "127.0.0.1", "7070", "33222", "job",
                1.0 * System.currentTimeMillis(), 1.0 * 29L, 1.0 * 10, 1.0 * 5);
        series.setValues(Lists.newArrayList());
        series.getValues().add(value);

        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Lists.newArrayList(series));

        QueryResult queryResult = new QueryResult();
        queryResult.setResults(Lists.newArrayList(result));
        return queryResult;
    }

    @Test
    void testWrite2InfluxDB() {
        MonitorDao monitorDao = new MonitorDao(influxDBInstance);

        boolean result = monitorDao.write2InfluxDB(monitorDao.convert2InfluxDBWriteRequest(mockQueryMonitorMetric()));
        Assertions.assertTrue(result);

        result = monitorDao.write2InfluxDB(monitorDao.convert2InfluxDBWriteRequest(mockJobStatusMonitorMetric()));
        Assertions.assertFalse(result);
    }

    @Test
    void testReadQueryMonitorMetricFromInfluxDB() {
        MonitorDao monitorDao = new MonitorDao(influxDBInstance);

        List<QueryMonitorMetric> monitorMetric = monitorDao.readQueryMonitorMetricFromInfluxDB(0L, Long.MAX_VALUE);
        Assertions.assertEquals("localhost", monitorMetric.get(0).getHost());
        Assertions.assertEquals(Integer.valueOf(2), monitorMetric.get(0).getErrorAccumulated());
    }

    @Test
    void testReadJobStatusMonitorMetricFromInfluxDB() {
        MonitorDao monitorDao = new MonitorDao(influxDBInstance);

        List<JobStatusMonitorMetric> monitorMetric = monitorDao.readJobStatusMonitorMetricFromInfluxDB(0L,
                Long.MAX_VALUE);
        Assertions.assertEquals("33222", monitorMetric.get(0).getPid());
        Assertions.assertEquals(Long.valueOf(5), monitorMetric.get(0).getErrorJobs());
    }
}
