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
import java.util.List;

import org.apache.commons.lang3.SerializationException;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.favorite.FavoriteRule.AbstractCondition;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

public class FavoriteRuleTable extends SqlTable {
    public final SqlColumn<Integer> id = column("id", JDBCType.INTEGER);
    public final SqlColumn<String> project = column("project", JDBCType.VARCHAR);
    public final SqlColumn<List<AbstractCondition>> conds = column("conds", JDBCType.VARCHAR,
            ConditionsHandler.class.getName());
    public final SqlColumn<String> name = column("name", JDBCType.VARCHAR);
    public final SqlColumn<Boolean> enabled = column("enabled", JDBCType.BOOLEAN);
    public final SqlColumn<Long> updateTime = column("update_time", JDBCType.BIGINT);
    public final SqlColumn<Long> createTime = column("create_time", JDBCType.BIGINT);
    public final SqlColumn<Long> mvcc = column("mvcc", JDBCType.BIGINT);

    protected FavoriteRuleTable(String tableName) {
        super(tableName);
    }

    public static class ConditionsHandler implements TypeHandler<List<AbstractCondition>> {

        @Override
        public void setParameter(PreparedStatement ps, int i, List<AbstractCondition> parameter, JdbcType jdbcType)
                throws SQLException {
            Preconditions.checkArgument(parameter != null, "conds cannot be null");
            try {
                TypeReference<List<AbstractCondition>> typeRef = new TypeReference<List<AbstractCondition>>() {
                };
                ps.setString(i, JsonUtil.writeValueAsStringForCollection(parameter, typeRef));
            } catch (JsonProcessingException e) {
                throw new SerializationException("cannot serialize conds", e);
            }
        }

        @Override
        public List<AbstractCondition> getResult(ResultSet rs, String columnName) throws SQLException {
            return toConditions(rs.getString(columnName));
        }

        @Override
        public List<AbstractCondition> getResult(ResultSet rs, int columnIndex) throws SQLException {
            return toConditions(rs.getString(columnIndex));
        }

        @Override
        public List<AbstractCondition> getResult(CallableStatement cs, int columnIndex) throws SQLException {
            return toConditions(cs.getString(columnIndex));
        }

        List<AbstractCondition> toConditions(String jsonString) {
            TypeReference<List<AbstractCondition>> typeRef = new TypeReference<List<AbstractCondition>>() {
            };
            try {
                return JsonUtil.readValue(jsonString, typeRef);
            } catch (IOException e) {
                throw new IllegalStateException("cannot deserialize depend id correctly", e);
            }
        }

    }
}
