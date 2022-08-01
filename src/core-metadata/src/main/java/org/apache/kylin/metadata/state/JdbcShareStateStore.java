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

package org.apache.kylin.metadata.state;

import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcShareStateStore {
    private final ShareStateTable shareStateTable;
    @Getter
    private final SqlSessionFactory sqlSessionFactory;
    private final DataSource dataSource;
    String ssTableName;

    public static JdbcShareStateStore getInstance() {
        return Singletons.getInstance(JdbcShareStateStore.class);
    }

    private JdbcShareStateStore() throws Exception {
        StorageURL url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        ssTableName = StorageURL.replaceUrl(url) + "_" + "share_state";
        shareStateTable = new ShareStateTable(ssTableName);
        sqlSessionFactory = ShareStateUtil.getSqlSessionFactory(dataSource, ssTableName);
    }

    public int insert(String instanceName, String shareState) {
        try(SqlSession session = sqlSessionFactory.openSession()) {
            ShareStateMapper ssMapper = session.getMapper(ShareStateMapper.class);
            ShareStateInfo shareStateInfoObj = new ShareStateInfo(instanceName, shareState);
            InsertStatementProvider<ShareStateInfo> insertStatement = getInsertShareStateProvider(shareStateInfoObj);
            int rows = ssMapper.insert(insertStatement);
            log.debug("Insert {} items into database, instanceName:{}", rows, instanceName);
            session.commit();
            return rows;
        }
    }

    InsertStatementProvider<ShareStateInfo> getInsertShareStateProvider(ShareStateInfo shareStateInfo) {
        return SqlBuilder.insert(shareStateInfo).into(shareStateTable)
                .map(shareStateTable.instanceName).toPropertyWhenPresent("instanceName", shareStateInfo::getInstanceName) //
                .map(shareStateTable.shareState).toPropertyWhenPresent("shareState", shareStateInfo::getShareState) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    public void update(String instanceName, String shareState) {
        try(SqlSession session = sqlSessionFactory.openSession()) {
            ShareStateMapper ssMapper = session.getMapper(ShareStateMapper.class);
            UpdateStatementProvider updateStatement = getUpdateShareStateProvider(instanceName, shareState);
            ssMapper.update(updateStatement);
            session.commit();
        }
    }

    private UpdateStatementProvider getUpdateShareStateProvider(String instanceName, String shareState) {
        return SqlBuilder.update(shareStateTable)
                .set(shareStateTable.shareState).equalTo(shareState)
                .where(shareStateTable.instanceName, isEqualTo(instanceName))
                .build().render(RenderingStrategies.MYBATIS3);
    }

    public ShareStateInfo selectShareStateByInstanceName(String instanceName) {
        try(SqlSession session = sqlSessionFactory.openSession()) {
            ShareStateMapper ssMapper = session.getMapper(ShareStateMapper.class);
            SelectStatementProvider selectStatement = getSelectShareStateProvider(instanceName);
            return ssMapper.selectOne(selectStatement);
        }
    }

    private SelectStatementProvider getSelectShareStateProvider(String instanceName) {
        return SqlBuilder.select(getSelectFields(shareStateTable))
                .from(shareStateTable)
                .where(shareStateTable.instanceName, isEqualTo(instanceName))
                .limit(1)
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(ShareStateTable shareStateTable) {
        return BasicColumn.columnList(shareStateTable.instanceName, shareStateTable.shareState);
    }

}
