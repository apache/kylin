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

package org.apache.kylin.metrics.property;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.junit.Test;

public class MetricsPropertyEnumTest {

    @Test
    public void testJobPropertyEnum() {
        assertEquals(JobPropertyEnum.ID_CODE, JobPropertyEnum.getByName("JOB_ID"));
        assertEquals(JobPropertyEnum.USER, JobPropertyEnum.getByName("KUSER"));
        assertEquals(JobPropertyEnum.PROJECT, JobPropertyEnum.getByName("PROJECT"));
        assertEquals(JobPropertyEnum.CUBE, JobPropertyEnum.getByName("CUBE_NAME"));
        assertEquals(JobPropertyEnum.TYPE, JobPropertyEnum.getByName("JOB_TYPE"));
        assertEquals(JobPropertyEnum.ALGORITHM, JobPropertyEnum.getByName("CUBING_TYPE"));
        assertEquals(JobPropertyEnum.STATUS, JobPropertyEnum.getByName("JOB_STATUS"));
        assertEquals(JobPropertyEnum.EXCEPTION, JobPropertyEnum.getByName("EXCEPTION"));
        assertEquals(JobPropertyEnum.SOURCE_SIZE, JobPropertyEnum.getByName("TABLE_SIZE"));
        assertEquals(JobPropertyEnum.CUBE_SIZE, JobPropertyEnum.getByName("CUBE_SIZE"));
        assertEquals(JobPropertyEnum.BUILD_DURATION, JobPropertyEnum.getByName("DURATION"));
        assertEquals(JobPropertyEnum.WAIT_RESOURCE_TIME, JobPropertyEnum.getByName("WAIT_RESOURCE_TIME"));
        assertEquals(JobPropertyEnum.PER_BYTES_TIME_COST, JobPropertyEnum.getByName("PER_BYTES_TIME_COST"));
        assertEquals(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS,
                JobPropertyEnum.getByName("STEP_DURATION_DISTINCT_COLUMNS"));
        assertEquals(JobPropertyEnum.STEP_DURATION_DICTIONARY, JobPropertyEnum.getByName("STEP_DURATION_DICTIONARY"));
        assertEquals(JobPropertyEnum.STEP_DURATION_INMEM_CUBING,
                JobPropertyEnum.getByName("STEP_DURATION_INMEM_CUBING"));
        assertEquals(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT,
                JobPropertyEnum.getByName("STEP_DURATION_HFILE_CONVERT"));
        assertNull(RecordEvent.RecordReserveKeyEnum.getByKey(null));
    }

    @Test
    public void testQueryPropertyEnum() {
        assertEquals(QueryPropertyEnum.ID_CODE, QueryPropertyEnum.getByName("QUERY_HASH_CODE"));
        assertEquals(QueryPropertyEnum.TYPE, QueryPropertyEnum.getByName("QUERY_TYPE"));
        assertEquals(QueryPropertyEnum.USER, QueryPropertyEnum.getByName("KUSER"));
        assertEquals(QueryPropertyEnum.PROJECT, QueryPropertyEnum.getByName("PROJECT"));
        assertEquals(QueryPropertyEnum.REALIZATION, QueryPropertyEnum.getByName("REALIZATION"));
        assertEquals(QueryPropertyEnum.REALIZATION_TYPE, QueryPropertyEnum.getByName("REALIZATION_TYPE"));
        assertEquals(QueryPropertyEnum.EXCEPTION, QueryPropertyEnum.getByName("EXCEPTION"));
        assertEquals(QueryPropertyEnum.TIME_COST, QueryPropertyEnum.getByName("QUERY_TIME_COST"));
        assertEquals(QueryPropertyEnum.CALCITE_RETURN_COUNT, QueryPropertyEnum.getByName("CALCITE_COUNT_RETURN"));
        assertEquals(QueryPropertyEnum.STORAGE_RETURN_COUNT, QueryPropertyEnum.getByName("STORAGE_COUNT_RETURN"));
        assertEquals(QueryPropertyEnum.AGGR_FILTER_COUNT,
                QueryPropertyEnum.getByName("CALCITE_COUNT_AGGREGATE_FILTER"));
        assertNull(RecordEvent.RecordReserveKeyEnum.getByKey(null));
    }

    @Test
    public void testQueryCubePropertyEnum() {
        assertEquals(QueryCubePropertyEnum.PROJECT, QueryCubePropertyEnum.getByName("PROJECT"));
        assertEquals(QueryCubePropertyEnum.CUBE, QueryCubePropertyEnum.getByName("CUBE_NAME"));
        assertEquals(QueryCubePropertyEnum.SEGMENT, QueryCubePropertyEnum.getByName("SEGMENT_NAME"));
        assertEquals(QueryCubePropertyEnum.CUBOID_SOURCE, QueryCubePropertyEnum.getByName("CUBOID_SOURCE"));
        assertEquals(QueryCubePropertyEnum.CUBOID_TARGET, QueryCubePropertyEnum.getByName("CUBOID_TARGET"));
        assertEquals(QueryCubePropertyEnum.IF_MATCH, QueryCubePropertyEnum.getByName("IF_MATCH"));
        assertEquals(QueryCubePropertyEnum.FILTER_MASK, QueryCubePropertyEnum.getByName("FILTER_MASK"));
        assertEquals(QueryCubePropertyEnum.IF_SUCCESS, QueryCubePropertyEnum.getByName("IF_SUCCESS"));
        assertEquals(QueryCubePropertyEnum.TIME_SUM, QueryCubePropertyEnum.getByName("STORAGE_CALL_TIME_SUM"));
        assertEquals(QueryCubePropertyEnum.TIME_MAX, QueryCubePropertyEnum.getByName("STORAGE_CALL_TIME_MAX"));
        assertEquals(QueryCubePropertyEnum.WEIGHT_PER_HIT, QueryCubePropertyEnum.getByName("WEIGHT_PER_HIT"));
        assertEquals(QueryCubePropertyEnum.CALL_COUNT, QueryCubePropertyEnum.getByName("STORAGE_CALL_COUNT"));
        assertEquals(QueryCubePropertyEnum.SKIP_COUNT, QueryCubePropertyEnum.getByName("STORAGE_COUNT_SKIP"));
        assertEquals(QueryCubePropertyEnum.SCAN_COUNT, QueryCubePropertyEnum.getByName("STORAGE_COUNT_SCAN"));
        assertEquals(QueryCubePropertyEnum.RETURN_COUNT, QueryCubePropertyEnum.getByName("STORAGE_COUNT_RETURN"));
        assertEquals(QueryCubePropertyEnum.AGGR_FILTER_COUNT,
                QueryCubePropertyEnum.getByName("STORAGE_COUNT_AGGREGATE_FILTER"));
        assertEquals(QueryCubePropertyEnum.AGGR_COUNT, QueryCubePropertyEnum.getByName("STORAGE_COUNT_AGGREGATE"));
        assertNull(RecordEvent.RecordReserveKeyEnum.getByKey(null));
    }

    @Test
    public void testQueryRPCPropertyEnum() {
        assertEquals(QueryRPCPropertyEnum.PROJECT, QueryRPCPropertyEnum.getByName("PROJECT"));
        assertEquals(QueryRPCPropertyEnum.REALIZATION, QueryRPCPropertyEnum.getByName("REALIZATION"));
        assertEquals(QueryRPCPropertyEnum.RPC_SERVER, QueryRPCPropertyEnum.getByName("RPC_SERVER"));
        assertEquals(QueryRPCPropertyEnum.EXCEPTION, QueryRPCPropertyEnum.getByName("EXCEPTION"));
        assertEquals(QueryRPCPropertyEnum.CALL_TIME, QueryRPCPropertyEnum.getByName("CALL_TIME"));
        assertEquals(QueryRPCPropertyEnum.SKIP_COUNT, QueryRPCPropertyEnum.getByName("COUNT_SKIP"));
        assertEquals(QueryRPCPropertyEnum.SCAN_COUNT, QueryRPCPropertyEnum.getByName("COUNT_SCAN"));
        assertEquals(QueryRPCPropertyEnum.RETURN_COUNT, QueryRPCPropertyEnum.getByName("COUNT_RETURN"));
        assertEquals(QueryRPCPropertyEnum.AGGR_FILTER_COUNT, QueryRPCPropertyEnum.getByName("COUNT_AGGREGATE_FILTER"));
        assertEquals(QueryRPCPropertyEnum.AGGR_COUNT, QueryRPCPropertyEnum.getByName("COUNT_AGGREGATE"));
        assertNull(RecordEvent.RecordReserveKeyEnum.getByKey(null));
    }

    @Test
    public void testTimePropertyEnum() {
        assertEquals(TimePropertyEnum.YEAR, TimePropertyEnum.getByKey("KYEAR_BEGIN_DATE"));
        assertEquals(TimePropertyEnum.MONTH, TimePropertyEnum.getByKey("KMONTH_BEGIN_DATE"));
        assertEquals(TimePropertyEnum.WEEK_BEGIN_DATE, TimePropertyEnum.getByKey("KWEEK_BEGIN_DATE"));
        assertEquals(TimePropertyEnum.DAY_DATE, TimePropertyEnum.getByKey("KDAY_DATE"));
        assertEquals(TimePropertyEnum.DAY_TIME, TimePropertyEnum.getByKey("KDAY_TIME"));
        assertEquals(TimePropertyEnum.TIME_HOUR, TimePropertyEnum.getByKey("KTIME_HOUR"));
        assertEquals(TimePropertyEnum.TIME_MINUTE, TimePropertyEnum.getByKey("KTIME_MINUTE"));
        assertEquals(TimePropertyEnum.TIME_SECOND, TimePropertyEnum.getByKey("KTIME_SECOND"));
        assertNull(RecordEvent.RecordReserveKeyEnum.getByKey(null));
    }

    @Test
    public void testRecordReserveKeyEnum() {
        assertEquals(RecordEvent.RecordReserveKeyEnum.EVENT_SUBJECT,
                RecordEvent.RecordReserveKeyEnum.getByKey("EVENT_TYPE"));
        assertEquals(RecordEvent.RecordReserveKeyEnum.ID, RecordEvent.RecordReserveKeyEnum.getByKey("EVENT_ID"));
        assertEquals(RecordEvent.RecordReserveKeyEnum.HOST, RecordEvent.RecordReserveKeyEnum.getByKey("HOST"));
        assertEquals(RecordEvent.RecordReserveKeyEnum.TIME, RecordEvent.RecordReserveKeyEnum.getByKey("KTIMESTAMP"));
        assertNull(RecordEvent.RecordReserveKeyEnum.getByKey(null));
    }
}