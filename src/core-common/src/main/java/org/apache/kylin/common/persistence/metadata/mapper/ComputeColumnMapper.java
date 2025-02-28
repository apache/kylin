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
package org.apache.kylin.common.persistence.metadata.mapper;

import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.ID_FIELD;
import static org.apache.kylin.common.persistence.metadata.mapper.ComputeColumnDynamicSqlSupport.sqlTable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectKey;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.persistence.metadata.jdbc.ContentTypeHandler;
import org.apache.kylin.common.persistence.metadata.jdbc.SqlWithRecordLockProviderAdapter;
import org.apache.kylin.common.persistence.resources.ComputeColumnRawResource;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.UpdateDSL;
import org.mybatis.dynamic.sql.update.UpdateModel;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;
import org.mybatis.dynamic.sql.util.mybatis3.MyBatis3Utils;

public interface ComputeColumnMapper extends BasicMapper<ComputeColumnRawResource> {
    @Override
    default BasicColumn[] getSelectList() {
        return getSelectListWithAdditions(sqlTable.columnName, sqlTable.tableIdentity,
                sqlTable.tableAlias, sqlTable.expression, sqlTable.innerExpression, sqlTable.datatype,
                sqlTable.referenceCount, sqlTable.ccComment);
    }

    @Override
    default BasicSqlTable getSqlTable() {
        return sqlTable;
    }

    @Override
    default UpdateDSL<UpdateModel> updateAllColumns(ComputeColumnRawResource record, UpdateDSL<UpdateModel> dsl) {
        dsl = BasicMapper.super.updateAllColumns(record, dsl);
        return dsl.set(sqlTable.columnName).equalTo(record::getColumnName).set(sqlTable.tableIdentity)
                .equalTo(record::getTableIdentity).set(sqlTable.tableAlias).equalTo(record::getTableAlias)
                .set(sqlTable.expression).equalTo(record::getExpression).set(sqlTable.innerExpression)
                .equalTo(record::getInnerExpression).set(sqlTable.datatype).equalTo(record::getDatatype)
                .set(sqlTable.referenceCount).equalTo(record::getReferenceCount).set(sqlTable.ccComment)
                .equalTo(record::getCcComment).set(sqlTable.expressionMd5).equalTo(record.getExpressionMd5());
    }

    @SuppressWarnings("unchecked")
    @Override
    default int insert(ComputeColumnRawResource record) {
        return MyBatis3Utils.insert(this::insert, record, getSqlTable(), c -> {
            getSelectColumnMap().forEach((k, v) -> {
                if (k.equals(ID_FIELD)) {
                    return;
                }
                // Currently the insert and update are all fields inserted, so there is no need to use `WhenPresent`
                c.map((SqlColumn<Object>) v).toProperty(k);
            });
            c.map(sqlTable.expressionMd5).toProperty("expressionMd5");
            return c;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    default int insertMultiple(Collection<ComputeColumnRawResource> records) {
        return MyBatis3Utils.insertMultiple(this::insertMultiple, records, getSqlTable(), c -> {
            getSelectColumnMap().forEach((k, v) -> {
                if (k.equals(ID_FIELD)) {
                    return;
                }
                // Currently the insert and update are all fields inserted, so there is no need to use `WhenPresent`
                c.map((SqlColumn<Object>) v).toProperty(k);
            });
            c.map(sqlTable.expressionMd5).toProperty("expressionMd5");
            return c;
        });
    }

    @Override
    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @ResultMap("ComputeColumnResult")
    Optional<ComputeColumnRawResource> selectOne(SelectStatementProvider selectStatement);

    @Override
    @SelectProvider(type = SqlProviderAdapter.class, method = "select")
    @Results(id = "ComputeColumnResult", value = {
            @Result(column = "id", property = "id", jdbcType = JdbcType.BIGINT, id = true),
            @Result(column = "meta_key", property = "metaKey", jdbcType = JdbcType.VARCHAR),
            @Result(column = "uuid", property = "uuid", jdbcType = JdbcType.CHAR),
            @Result(column = "project", property = "project", jdbcType = JdbcType.VARCHAR),
            @Result(column = "column_name", property = "columnName", jdbcType = JdbcType.VARCHAR),
            @Result(column = "table_identity", property = "tableIdentity", jdbcType = JdbcType.VARCHAR),
            @Result(column = "table_alias", property = "tableAlias", jdbcType = JdbcType.VARCHAR),
            @Result(column = "expression", property = "expression", jdbcType = JdbcType.VARCHAR),
            @Result(column = "inner_expression", property = "innerExpression", jdbcType = JdbcType.VARCHAR),
            @Result(column = "datatype", property = "datatype", jdbcType = JdbcType.VARCHAR),
            @Result(column = "reference_count", property = "referenceCount", jdbcType = JdbcType.INTEGER),
            @Result(column = "cc_comment", property = "ccComment", jdbcType = JdbcType.VARCHAR),
            @Result(column = "mvcc", property = "mvcc", jdbcType = JdbcType.BIGINT),
            @Result(column = "ts", property = "ts", jdbcType = JdbcType.BIGINT),
            @Result(column = "reserved_filed_1", property = "reservedFiled1", jdbcType = JdbcType.VARCHAR),
            @Result(column = "content", property = "content", jdbcType = JdbcType.LONGVARBINARY, typeHandler = ContentTypeHandler.class),
            @Result(column = "reserved_filed_2", property = "reservedFiled2", jdbcType = JdbcType.LONGVARBINARY),
            @Result(column = "reserved_filed_3", property = "reservedFiled3", jdbcType = JdbcType.LONGVARBINARY) })
    List<ComputeColumnRawResource> selectMany(SelectStatementProvider selectStatement);

    @Override
    @SelectProvider(type = SqlWithRecordLockProviderAdapter.class, method = "select")
    @ResultMap("ComputeColumnResult")
    List<ComputeColumnRawResource> selectManyWithRecordLock(SelectStatementProvider selectStatement);
}
