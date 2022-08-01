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

import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;

@Mapper
public interface QueryHistoryMapper {

    @DeleteProvider(type = SqlProviderAdapter.class, method = "delete")
    int delete(DeleteStatementProvider deleteStatement);

    @InsertProvider(type = SqlProviderAdapter.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "record.id")
    int insert(InsertStatementProvider<QueryMetrics> insertStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "QueryHistoryResult", value = {
            @Result(column = "sql_text", property = "sql", jdbcType = JdbcType.VARCHAR),
            @Result(column = "sql_pattern", property = "sqlPattern", jdbcType = JdbcType.VARCHAR),
            @Result(column = "query_time", property = "queryTime", jdbcType = JdbcType.BIGINT),
            @Result(column = "duration", property = "duration", jdbcType = JdbcType.BIGINT),
            @Result(column = "realizations", property = "queryRealizations", jdbcType = JdbcType.VARCHAR),
            @Result(column = "server", property = "hostName", jdbcType = JdbcType.VARCHAR),
            @Result(column = "submitter", property = "querySubmitter", jdbcType = JdbcType.VARCHAR),
            @Result(column = "query_status", property = "queryStatus", jdbcType = JdbcType.VARCHAR),
            @Result(column = "query_id", property = "queryId", jdbcType = JdbcType.VARCHAR),
            @Result(column = "id", property = "id", jdbcType = JdbcType.BIGINT),
            @Result(column = "total_scan_count", property = "totalScanCount", jdbcType = JdbcType.BIGINT),
            @Result(column = "total_scan_bytes", property = "totalScanBytes", jdbcType = JdbcType.BIGINT),
            @Result(column = "result_row_count", property = "resultRowCount", jdbcType = JdbcType.BIGINT),
            @Result(column = "cache_hit", property = "cacheHit", jdbcType = JdbcType.BOOLEAN),
            @Result(column = "index_hit", property = "indexHit", jdbcType = JdbcType.BOOLEAN),
            @Result(column = "engine_type", property = "engineType", jdbcType = JdbcType.VARCHAR),
            @Result(column = "project_name", property = "projectName", jdbcType = JdbcType.VARCHAR),
            @Result(column = "error_type", property = "errorType", jdbcType = JdbcType.VARCHAR),
            @Result(column = "reserved_field_3", property = "queryHistoryInfo", jdbcType = JdbcType.BLOB, typeHandler = QueryHistoryTable.QueryHistoryInfoHandler.class) })
    List<QueryHistory> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("QueryHistoryResult")
    QueryHistory selectOne(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    Long selectAsLong(SelectStatementProvider selectStatement);

    @UpdateProvider(type = SqlProviderAdapter.class, method = "update")
    int update(UpdateStatementProvider updateStatement);

}
