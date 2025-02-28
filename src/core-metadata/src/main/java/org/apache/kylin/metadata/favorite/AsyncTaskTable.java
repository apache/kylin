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

package org.apache.kylin.metadata.favorite;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.SerializationException;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;

import com.fasterxml.jackson.core.JsonProcessingException;

public class AsyncTaskTable extends SqlTable {

    public final SqlColumn<Integer> id = column("id", JDBCType.INTEGER);
    public final SqlColumn<String> project = column("project", JDBCType.VARCHAR);
    public final SqlColumn<AbstractAsyncTask.TaskAttributes> taskAttributes = column("task_attributes",
            JDBCType.VARCHAR, TaskHandler.class.getName());
    public final SqlColumn<String> taskType = column("task_type", JDBCType.VARCHAR);
    public final SqlColumn<String> taskKey = column("task_key", JDBCType.VARCHAR);
    public final SqlColumn<Long> updateTime = column("update_time", JDBCType.BIGINT);
    public final SqlColumn<Long> createTime = column("create_time", JDBCType.BIGINT);
    public final SqlColumn<Long> mvcc = column("mvcc", JDBCType.BIGINT);

    protected AsyncTaskTable(String tableName) {
        super(tableName);
    }

    public static class TaskHandler implements TypeHandler<AbstractAsyncTask.TaskAttributes> {

        @Override
        public void setParameter(PreparedStatement ps, int i, AbstractAsyncTask.TaskAttributes parameter,
                JdbcType jdbcType) throws SQLException {
            Preconditions.checkArgument(parameter != null, "task attributes cannot be null");
            try {
                ps.setString(i, JsonUtil.writeValueAsString(parameter));
            } catch (JsonProcessingException e) {
                throw new SerializationException("cannot serialize task attributes", e);
            }
        }

        @Override
        public AbstractAsyncTask.TaskAttributes getResult(ResultSet rs, String columnName) throws SQLException {
            return toTaskAttributes(rs.getString(columnName));
        }

        @Override
        public AbstractAsyncTask.TaskAttributes getResult(ResultSet rs, int columnIndex) throws SQLException {
            return toTaskAttributes(rs.getString(columnIndex));
        }

        @Override
        public AbstractAsyncTask.TaskAttributes getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return toTaskAttributes(cs.getString(columnIndex));
        }

        AbstractAsyncTask.TaskAttributes toTaskAttributes(String jsonString) {
            try {
                return JsonUtil.readValue(jsonString, AbstractAsyncTask.TaskAttributes.class);
            } catch (IOException e) {
                throw new IllegalStateException("cannot deserialize task correctly", e);
            }
        }
    }
}
