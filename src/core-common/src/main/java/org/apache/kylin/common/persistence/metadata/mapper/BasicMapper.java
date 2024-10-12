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
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.META_KEY_PROPERTIES_NAME;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.metadata.MetadataMapperFactory;
import org.apache.kylin.common.persistence.metadata.jdbc.SqlWithRecordLockProviderAdapter;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.delete.DeleteDSLCompleter;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.insert.render.MultiRowInsertStatementProvider;
import org.mybatis.dynamic.sql.render.TableAliasCalculator;
import org.mybatis.dynamic.sql.select.SelectDSLCompleter;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.UpdateDSL;
import org.mybatis.dynamic.sql.update.UpdateDSLCompleter;
import org.mybatis.dynamic.sql.update.UpdateModel;
import org.mybatis.dynamic.sql.util.SqlProviderAdapter;
import org.mybatis.dynamic.sql.util.mybatis3.CommonCountMapper;
import org.mybatis.dynamic.sql.util.mybatis3.CommonDeleteMapper;
import org.mybatis.dynamic.sql.util.mybatis3.CommonUpdateMapper;
import org.mybatis.dynamic.sql.util.mybatis3.MyBatis3Utils;

/**
 * 15/11/2023 hellozepp(lisheng.zhanglin@163.com)
 */
public interface BasicMapper<T extends RawResource> extends CommonCountMapper, CommonDeleteMapper, CommonUpdateMapper {

    Map<String, Map<String, BasicColumn>> SQL_FILED_CACHE = new ConcurrentHashMap<>();

    
    default boolean needProjectFiled() {
        return true;
    }

    <T2 extends BasicSqlTable<T2>> T2 getSqlTable();

    /**
     * This array is actually a polymorphic instance of the SqlColumn type and can be downcast to
     * SqlColumn or BindableColumn.
     * BasicColumn is obtained from the static constants of the mapper, it is immutable. It is used
     * in {@link  BasicColumn#renderWithTableAndColumnAlias(TableAliasCalculator)} for sql generation
     * . Currently, we do not set table alias, so even if the table name is modified, BasicColumn
     * will not be affected.
     * @return BasicColumn
     */
    default BasicColumn[] getSelectList() {
        return getSelectListWithAdditions();
    }

    default BasicColumn[] getSelectListWithAdditions(BasicColumn... additions) {
        List<BasicColumn> generalColumns = getSqlTable().getGeneralColumns();
        if (needProjectFiled()) {
            generalColumns.add(getSqlTable().project);
        }
        generalColumns.addAll(Arrays.asList(additions));
        return generalColumns.toArray(new BasicColumn[0]);
    }

    /**
     * Generate a map of column name to column object for use in select statements.
     * @return the map of camel case column names to column objects
     */
    default Map<String, BasicColumn> getSelectColumnMap() {
        BasicColumn[] selectList = getSelectList();
        String tableName = getSqlTable().tableNameAtRuntime();
        Map<String, BasicColumn> sqlFieldHelper;
        if (selectList.length > 0 && !SQL_FILED_CACHE.containsKey(tableName)) {
            sqlFieldHelper = Maps.newHashMap();
            for (BasicColumn col : selectList) {
                sqlFieldHelper.put(MetadataMapperFactory.snakeCaseToCamelCase(((SqlColumn<?>) col).name(), false), col);
            }
            // To reduce the probability of map conflicts
            if (!SQL_FILED_CACHE.containsKey(tableName)) {
                SQL_FILED_CACHE.put(tableName, sqlFieldHelper);
            }
        } else {
            sqlFieldHelper = SQL_FILED_CACHE.get(tableName);
        }
        return sqlFieldHelper;
    }

    default BasicColumn getSqlColumn(String fieldName) {
        return getSelectColumnMap().get(fieldName);
    }

    default int update(UpdateDSLCompleter completer) {
        return MyBatis3Utils.update(this::update, getSqlTable(), completer);
    }

    @SuppressWarnings("unchecked")
    default int updateByPrimaryKeyAndMvcc(RawResource record) {
        return MyBatis3Utils.update(this::update, getSqlTable(),
                c -> updateAllColumns((T) record, c)
                        .where((SqlColumn<String>) getSqlColumn(META_KEY_PROPERTIES_NAME),
                                isEqualTo(record.getMetaKey()))
                        .and(getSqlTable().getMvcc(), isEqualTo(record.getMvcc() - 1)));
    }

    @SuppressWarnings("unchecked")
    default UpdateDSL<UpdateModel> updateAllColumns(T record, UpdateDSL<UpdateModel> dsl) {
        BasicSqlTable<?> sqlTable = getSqlTable();
        dsl = dsl.set(sqlTable.uuid).equalTo(record.getUuid()).set(sqlTable.metaKey).equalTo(record.getMetaKey())
                .set(sqlTable.mvcc).equalTo(record.getMvcc()).set(sqlTable.ts).equalTo(record.getTs())
                .set(sqlTable.reservedFiled1).equalTo(record.getReservedFiled1()).set(sqlTable.content)
                .equalTo(record.getContent()).set(sqlTable.reservedFiled2).equalTo(record.getReservedFiled2())
                .set(sqlTable.reservedFiled3).equalTo(record.getReservedFiled3());
        if (needProjectFiled()) {
            dsl = dsl.set(sqlTable.project).equalTo(record.getProject());
        }
        return dsl;
    }

    @SuppressWarnings("unchecked")
    default int insertOne(RawResource record) {
        return insert((T) record);
    }

    default int delete(DeleteDSLCompleter completer) {
        return MyBatis3Utils.deleteFrom(this::delete, getSqlTable(), completer);
    }

    default Optional<T> selectOneWithColumnsAndRecordLock(SelectDSLCompleter completer, BasicColumn[] selectCols) {
        return MyBatis3Utils.selectOne(this::selectOneWithRecordLock, selectCols, getSqlTable(), completer);
    }

    List<T> selectMany(SelectStatementProvider selectStatementProvider);

    List<T> selectManyWithRecordLock(SelectStatementProvider selectStatement);

    @InsertProvider(type = SqlProviderAdapter.class, method = "insert")
    int insert(InsertStatementProvider<T> insertStatement);

    Optional<T> selectOne(SelectStatementProvider selectStatement);

    @SelectProvider(type = SqlWithRecordLockProviderAdapter.class, method = "select")
    Optional<T> selectOneWithRecordLock(SelectStatementProvider selectStatement);


    default List<T> select(SelectDSLCompleter completer) {
        return MyBatis3Utils.selectList(this::selectMany, getSelectList(), getSqlTable(), completer);
    }

    @SuppressWarnings("unchecked")
    default int insert(T record) {
        return MyBatis3Utils.insert(this::insert, record, getSqlTable(), c -> {
            getSelectColumnMap().forEach((k, v) -> {
                if (k.equals(ID_FIELD)) {
                    return;
                }
                // Currently the insert and update are all fields inserted, so there is no need to use `WhenPresent`
                c.map((SqlColumn<Object>) v).toProperty(k);
            });
            return c;
        });
    }

    @InsertProvider(type = SqlProviderAdapter.class, method = "insertMultiple")
    int insertMultiple(MultiRowInsertStatementProvider<T> multipleInsertStatement);

    @SuppressWarnings("unchecked")
    default int insertMultiple(Collection<T> records) {
        return MyBatis3Utils.insertMultiple(this::insertMultiple, records, getSqlTable(), c -> {
            getSelectColumnMap().forEach((k, v) -> {
                if (k.equals(ID_FIELD)) {
                    return;
                }
                // Currently the insert and update are all fields inserted, so there is no need to use `WhenPresent`
                c.map((SqlColumn<Object>) v).toProperty(k);
            });
            return c;
        });
    }
}
