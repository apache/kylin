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

import java.util.List;

import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.util.Pair;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;

@Mapper
public interface StreamingJobStatsMapper {

    @DeleteProvider(type = SqlProviderAdapter.class, method = "delete")
    int delete(DeleteStatementProvider deleteStatement);

    @InsertProvider(type = SqlProviderAdapter.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "record.id")
    int insert(InsertStatementProvider<StreamingJobStats> insertStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "StreamingJobStatsResult", value = {
            @Result(column = "id", property = "id", jdbcType = JdbcType.BIGINT),
            @Result(column = "project_name", property = "projectName", jdbcType = JdbcType.VARCHAR),
            @Result(column = "job_id", property = "jobId", jdbcType = JdbcType.VARCHAR),
            @Result(column = "batch_row_num", property = "batchRowNum", jdbcType = JdbcType.BIGINT),
            @Result(column = "rows_per_second", property = "rowsPerSecond", jdbcType = JdbcType.DOUBLE),
            @Result(column = "processing_time", property = "processingTime", jdbcType = JdbcType.BIGINT),
            @Result(column = "min_data_latency", property = "minDataLatency", jdbcType = JdbcType.BIGINT),
            @Result(column = "max_data_latency", property = "maxDataLatency", jdbcType = JdbcType.BIGINT),
            @Result(column = "create_time", property = "createTime", jdbcType = JdbcType.BIGINT) })
    List<StreamingJobStats> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "LastestJobIdResult", value = {
            @Result(column = "job_id", property = "first", jdbcType = JdbcType.VARCHAR),
            @Result(column = "id", property = "second", jdbcType = JdbcType.BIGINT) })
    List<Pair<String, Long>> selectLatestJobId(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("StreamingJobStatsResult")
    StreamingJobStats selectOne(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "AvgConsumptionRateByCountResult", value = {
            @Result(column = "min_rate", property = "minRate", jdbcType = JdbcType.DOUBLE),
            @Result(column = "max_rate", property = "maxRate", jdbcType = JdbcType.DOUBLE),
            @Result(column = "count", property = "count", jdbcType = JdbcType.BIGINT) })
    ConsumptionRateStats selectStreamingStatistics(SelectStatementProvider selectStatement);

}
