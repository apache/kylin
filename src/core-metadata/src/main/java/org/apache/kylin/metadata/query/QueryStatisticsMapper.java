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

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;

@Mapper
public interface QueryStatisticsMapper {

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "QueryStatisticsResult", value = {
            @Result(column = "engine_type", property = "engineType", jdbcType = JdbcType.VARCHAR),
            @Result(column = "count", property = "count", jdbcType = JdbcType.BIGINT),
            @Result(column = "ratio", property = "ratio", jdbcType = JdbcType.DOUBLE),
            @Result(column = "mean", property = "meanDuration", jdbcType = JdbcType.DOUBLE),
            @Result(column = "model", property = "model", jdbcType = JdbcType.VARCHAR),
            @Result(column = "time", property = "time", jdbcType = JdbcType.BIGINT, typeHandler = QueryHistoryTable.InstantHandler.class),
            @Result(column = "month", property = "month", jdbcType = JdbcType.VARCHAR) })
    List<QueryStatistics> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("QueryStatisticsResult")
    QueryStatistics selectOne(SelectStatementProvider selectStatement);

    @Select({ "<script>", //
            "select query_day as queryDay, count(1) as totalNum, ", //
            "sum(CASE WHEN query_status = 'SUCCEEDED' THEN 1 ELSE 0 END) as succeedNum, ", //
            "count(distinct submitter) as activeUserNum, ", //
            "sum(CASE WHEN query_status = 'SUCCEEDED' THEN duration ELSE 0 END) as totalDuration, ", //
            "sum(CASE WHEN duration &lt; 1000 THEN 1 ELSE 0 END) as lt1sNum, ", //
            "sum(CASE WHEN duration &lt; 3000 THEN 1 ELSE 0 END) as lt3sNum, ", //
            "sum(CASE WHEN duration &lt; 5000 THEN 1 ELSE 0 END) as lt5sNum, ", //
            "sum(CASE WHEN duration &lt; 10000 THEN 1 ELSE 0 END) as lt10sNum, ", //
            "sum(CASE WHEN duration &lt; 15000 THEN 1 ELSE 0 END) as lt15sNum ", //
            "from ${table} ", //
            "where query_day &gt;= #{start} and query_day &lt; #{end} ", //
            "group by query_day ", //
            "order by query_day desc ", //
            "</script>" })
    List<QueryDailyStatistic> selectDaily(@Param("table") String qhTable, @Param("start") long startTime,
            @Param("end") long endTime);
}
