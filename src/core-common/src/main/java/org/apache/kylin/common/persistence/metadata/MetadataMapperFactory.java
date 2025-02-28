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
package org.apache.kylin.common.persistence.metadata;

import static org.apache.kylin.common.persistence.MetadataType.NEED_CACHED_METADATA;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.metadata.mapper.BasicMapper;
import org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.BindableColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.VisitableCondition;
import org.mybatis.dynamic.sql.select.SelectDSLCompleter;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;

import com.google.common.base.Preconditions;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

/**
 * 15/11/2023 hellozepp(lisheng.zhanglin@163.com)
 */
@Slf4j
public class MetadataMapperFactory {

    private MetadataMapperFactory() {
    }

    @SuppressWarnings("unchecked")
    public static <T extends RawResource> BasicMapper<T> createFor(MetadataType type, SqlSession session) {
        return (BasicMapper<T>) session.getMapper(getMapperClass(type));
    }

    /**
     * @param dataSource
     * @return
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource)
            throws SQLException, IOException, ClassNotFoundException {
        log.info("Start to build SqlSessionFactory");
        TransactionFactory transactionFactory = new SpringManagedTransactionFactory();
        Environment environment = new Environment("kylin metadata store", transactionFactory, dataSource);
        Configuration configuration = new Configuration(environment);
        configuration.setUseGeneratedKeys(true);
        configuration.setJdbcTypeForNull(JdbcType.NULL);
        NEED_CACHED_METADATA.stream().filter(type -> type != MetadataType.TMP_REC)
                .map(MetadataMapperFactory::getMapperClass).forEach(configuration::addMapper);
        configuration.setCacheEnabled(false);
        configuration.setLogImpl(NoLoggingImpl.class);
        return new SqlSessionFactoryBuilder().build(configuration);
    }

    @SuppressWarnings("unchecked")
    private static Class<BasicMapper<? extends RawResource>> getMapperClass(MetadataType type) {
        try {
            return (Class<BasicMapper<? extends RawResource>>) Class.forName(BasicMapper.class.getPackage().getName()
                    + "." + snakeCaseToCamelCase(type.name().toLowerCase(Locale.ROOT), true) + "Mapper");
        } catch (ClassNotFoundException e) {
            throw new KylinRuntimeException("Cannot find mapper class for " + type, e);
        }
    }

    /**
     * Convert snake_case to camelCase
     * @param snakeCase snake_case
     * @return camelCase
     */
    public static String snakeCaseToCamelCase(String snakeCase, boolean firstCharUpper) {
        StringBuilder sb = new StringBuilder(snakeCase.length() + 1);
        boolean capNext = firstCharUpper;
        for (int ctr = 0; ctr < snakeCase.length(); ctr++) {
            char next = snakeCase.charAt(ctr);
            if (next == '_') {
                capNext = true;
            } else if (Character.isDigit(next)) {
                sb.append(next);
                capNext = true;
            } else if (capNext) {
                sb.append(Character.toUpperCase(next));
                capNext = false;
            } else if (ctr == 0) {
                sb.append(Character.toLowerCase(next));
            } else {
                sb.append(next);
            }
        }
        return sb.toString();
    }

    /**
     * Convert the common RawResourceFilter to SQL DSL, if the query conditions are not set, the DSL that queries
     *     all will be generated.
     * @param filter RawResourceFilter
     * @param selectColumnMap need to be selected column map
     * @return SelectDSLCompleter
     */
    public static SelectDSLCompleter convertConditionsToDSLCompleter(RawResourceFilter filter,
            Map<String, BasicColumn> selectColumnMap) {
        if (filter == null || filter.getConditions().isEmpty()) {
            return SelectDSLCompleter.allRows();
        }
        return c -> {
            var stat = c.where();
            boolean firstCondition = true;
            for (RawResourceFilter.Condition condition : filter.getConditions()) {

                assert condition.getOp() != null && condition.getValues() != null && !condition.getValues().isEmpty()
                        : "Invalid condition: " + condition;

                var curColumn = (BindableColumn) selectColumnMap.get(condition.getName());
                if (curColumn != null) {
                    VisitableCondition<?> vc = isEqualTo(condition.getValues().get(0));
                    switch (condition.getOp()) {
                    case EQUAL:
                        break;
                    case EQUAL_CASE_INSENSITIVE:
                        curColumn = SqlBuilder.upper(curColumn);
                        vc = isEqualTo(condition.getValues().stream().map(String::valueOf)
                                .map(s -> s.toUpperCase(Locale.ROOT)).findFirst().orElse(""));
                        break;
                    case IN:
                        vc = SqlBuilder.isIn(condition.getValues());
                        break;
                    case GT:
                        vc = SqlBuilder.isGreaterThan(condition.getValues().get(0));
                        break;
                    case LT:
                        vc = SqlBuilder.isLessThan(condition.getValues().get(0));
                        break;
                    case LIKE_CASE_INSENSITIVE:
                        vc = SqlBuilder.isLikeCaseInsensitive(String.valueOf(condition.getValues().get(0)));
                        break;
                    case LE:
                        vc = SqlBuilder.isLessThanOrEqualTo(condition.getValues().get(0));
                        break;
                    case GE:
                        vc = SqlBuilder.isGreaterThanOrEqualTo(condition.getValues().get(0));
                        break;
                    default:
                        throw new UnsupportedOperationException("Operator not supported: " + condition.getOp());
                    }

                    if (firstCondition) {
                        stat = c.where(curColumn, vc);
                        firstCondition = false;
                    } else {
                        stat.and(curColumn, vc);
                    }
                } else {
                    // Should not happen
                    throw new IllegalArgumentException("Invalid condition: " + condition);
                }
            }
            return stat;
        };
    }

    /**
     * Only for testing
     * When metadata jdbc url is changed by {@link KylinConfigBase#setMetadataUrl(String)}, we need to reset the mapper
     * table name to avoid the conflict.
     * @param url StorageURL
     * @param factory SqlSessionFactory
     */
    public static void resetMapperTableNameIfNeed(StorageURL url, SqlSessionFactory factory) {
        Preconditions.checkArgument("jdbc".equals(url.getScheme()));
        try (SqlSession session = factory.openSession()) {
            NEED_CACHED_METADATA.stream().filter(type -> type != MetadataType.TMP_REC).forEach(type -> {
                val mapper = MetadataMapperFactory.createFor(type, session);
                BasicSqlTable<?> sqlTable = mapper.getSqlTable();
                String identifier = url.getIdentifier();
                if (!sqlTable.getTableNamePrefix().equals(identifier)) {
                    String tableName = identifier + "_" + sqlTable.getTableNameSuffix();
                    log.warn("Reset the table name for metadata mapper: {}", tableName);
                    sqlTable.updateTableName();
                }
            });
        }
    }
}
