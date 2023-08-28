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

package org.apache.kylin.job.metrics;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;

import java.util.List;

@Mapper
public interface JobMetricsStatisticsMapper {

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "JobMetricsStatisticsResult", value = {
            @Result(column = "count", property = "count", jdbcType = JdbcType.INTEGER),
            @Result(column = "duration", property = "duration", jdbcType = JdbcType.BIGINT),
            @Result(column = "model_size", property = "modelSize", jdbcType = JdbcType.BIGINT),
            @Result(column = "time", property = "time", jdbcType = JdbcType.BIGINT, typeHandler = JobMetricsTable.InstantHandler.class),
            @Result(column = "model", property = "model", jdbcType = JdbcType.VARCHAR) })
    List<JobMetricsStatistics> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("JobMetricsStatisticsResult")
    JobMetricsStatistics selectOne(SelectStatementProvider selectStatement);

}
