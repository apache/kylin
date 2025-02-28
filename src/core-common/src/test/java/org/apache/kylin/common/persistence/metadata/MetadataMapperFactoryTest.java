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

import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE;
import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE;

import java.util.Arrays;
import java.util.Collections;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.metadata.mapper.BasicMapper;
import org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable;
import org.apache.kylin.common.persistence.metadata.mapper.ProjectDynamicSqlSupport;
import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.select.SelectDSLCompleter;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.util.mybatis3.MyBatis3Utils;
import org.mybatis.spring.SqlSessionTemplate;

@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
class MetadataMapperFactoryTest {
    KylinConfig config;
    DataSource dataSource;

    SqlSessionTemplate sqlSessionTemplate;

    @BeforeEach
    void setUp() throws Exception {
        config = KylinConfig.getInstanceFromEnv();
        dataSource = JdbcDataSource.getDataSource(JdbcUtil.datasourceParameters(config.getMetadataUrl()));
        SqlSessionFactory sqlSessionFactory = MetadataMapperFactory.getSqlSessionFactory(dataSource);
        sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
    }

    @Test
    void testConditionConverter() {
        BasicMapper<ProjectRawResource> mapper = MetadataMapperFactory.createFor(MetadataType.PROJECT,
                sqlSessionTemplate);
        RawResourceFilter filter = new RawResourceFilter();
        filter.addConditions("uuid", Arrays.asList("uuid_p1", "uuid_p2"), RawResourceFilter.Operator.IN);
        filter.addConditions("mvcc", Collections.singletonList(4L), RawResourceFilter.Operator.LT);
        filter.addConditions("mvcc", Collections.singletonList(0L), RawResourceFilter.Operator.LE);
        filter.addConditions("ts", Collections.singletonList(100L), RawResourceFilter.Operator.GT);
        filter.addConditions("ts", Collections.singletonList(100L), RawResourceFilter.Operator.GE);
        filter.addConditions("name", Collections.singletonList("p1"), RawResourceFilter.Operator.EQUAL);
        filter.addConditions("metaKey", Collections.singletonList("p1"), EQUAL_CASE_INSENSITIVE);
        filter.addConditions("metaKey", Collections.singletonList("p"), LIKE_CASE_INSENSITIVE);

        SelectDSLCompleter dsl = MetadataMapperFactory.convertConditionsToDSLCompleter(filter,
                mapper.getSelectColumnMap());
        SelectStatementProvider provider = MyBatis3Utils.select(mapper.getSelectList(), mapper.getSqlTable(), dsl);
        Assertions.assertEquals(9, provider.getParameters().size());
    }

    @Test
    void testInvalidCondition() {
        BasicMapper<ProjectRawResource> mapper = MetadataMapperFactory.createFor(MetadataType.PROJECT,
                sqlSessionTemplate);
        RawResourceFilter filter = RawResourceFilter.equalFilter("undefinedKey", "p1");
        SelectDSLCompleter dsl = MetadataMapperFactory.convertConditionsToDSLCompleter(filter,
                mapper.getSelectColumnMap());
        BasicColumn[] cols = mapper.getSelectList();
        BasicSqlTable<ProjectDynamicSqlSupport.Project> table = mapper.getSqlTable();
        Assertions.assertThrows(IllegalArgumentException.class, () -> MyBatis3Utils.select(cols, table, dsl));
    }
}
