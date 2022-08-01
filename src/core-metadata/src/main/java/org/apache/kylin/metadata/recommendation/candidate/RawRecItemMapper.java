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

package org.apache.kylin.metadata.recommendation.candidate;

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
public interface RawRecItemMapper {

    @DeleteProvider(type = SqlProviderAdapter.class, method = "delete")
    int delete(DeleteStatementProvider deleteStatement);

    @InsertProvider(type = SqlProviderAdapter.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "record.id")
    int insert(InsertStatementProvider<RawRecItem> insertStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "RawRecItemResult", value = {
            @Result(column = "id", property = "id", jdbcType = JdbcType.INTEGER, id = true),
            @Result(column = "project", property = "project", jdbcType = JdbcType.VARCHAR),
            @Result(column = "model_id", property = "modelID", jdbcType = JdbcType.VARCHAR),
            @Result(column = "unique_flag", property = "uniqueFlag", jdbcType = JdbcType.VARCHAR),
            @Result(column = "semantic_version", property = "semanticVersion", jdbcType = JdbcType.INTEGER),
            @Result(column = "type", property = "type", jdbcType = JdbcType.TINYINT, typeHandler = RawRecItemTable.RecTypeHandler.class),
            @Result(column = "rec_entity", property = "recEntity", jdbcType = JdbcType.VARCHAR, typeHandler = RawRecItemTable.RecItemV2Handler.class),
            @Result(column = "state", property = "state", jdbcType = JdbcType.TINYINT, typeHandler = RawRecItemTable.RecStateHandler.class),
            @Result(column = "create_time", property = "createTime", jdbcType = JdbcType.BIGINT),
            @Result(column = "update_time", property = "updateTime", jdbcType = JdbcType.BIGINT),
            @Result(column = "depend_ids", property = "dependIDs", jdbcType = JdbcType.VARCHAR, typeHandler = RawRecItemTable.DependIdHandler.class),
            @Result(column = "layout_metric", property = "layoutMetric", jdbcType = JdbcType.VARCHAR, typeHandler = RawRecItemTable.LayoutMetricHandler.class),
            @Result(column = "cost", property = "cost", jdbcType = JdbcType.DOUBLE),
            @Result(column = "total_latency_of_last_day", property = "totalLatencyOfLastDay", jdbcType = JdbcType.DOUBLE),
            @Result(column = "hit_count", property = "hitCount", jdbcType = JdbcType.INTEGER),
            @Result(column = "total_time", property = "totalTime", jdbcType = JdbcType.DOUBLE),
            @Result(column = "max_time", property = "maxTime", jdbcType = JdbcType.DOUBLE),
            @Result(column = "min_time", property = "minTime", jdbcType = JdbcType.DOUBLE),
            @Result(column = "query_history_info", property = "queryHistoryInfo", jdbcType = JdbcType.VARCHAR),
            @Result(column = "reserved_field_1", property = "recSource", jdbcType = JdbcType.VARCHAR) })
    List<RawRecItem> selectMany(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("RawRecItemResult")
    RawRecItem selectOne(SelectStatementProvider selectStatement);

    @UpdateProvider(type = SqlProviderAdapter.class, method = "update")
    int update(UpdateStatementProvider updateStatement);

    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    int selectAsInt(SelectStatementProvider selectStatement);
}
