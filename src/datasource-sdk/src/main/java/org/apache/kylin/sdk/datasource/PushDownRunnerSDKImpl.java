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
package org.apache.kylin.sdk.datasource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.adhocquery.IPushDownRunner;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PushDownRunnerSDKImpl implements IPushDownRunner {
    private JdbcConnector dataSource;

    @Override
    public void init(KylinConfig config) {
        dataSource = SourceConnectorFactory.getJdbcConnector(config);
    }

    @Override
    public void init(KylinConfig config, String project) {
        init(config);
    }

    @Override
    public void executeQuery(String query, List<List<String>> returnRows, List<SelectedColumnMeta> returnColumnMeta,
            String project) throws SQLException {
        query = dataSource.convertSql(query);

        //extract column metadata
        ResultSet rs = null;
        ResultSetMetaData metaData;
        int columnCount;
        String dbName = null;
        if (StringUtils.isNotBlank(project)) {
            dbName = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getDefaultDatabase(project);
        }
        try (Connection conn = dataSource.getConnectionWithDefaultDb(dbName);
                Statement state = conn.createStatement()) {
            log.info("SDK push down sql is [{}]", query);
            rs = state.executeQuery(query);

            extractResults(rs, returnRows);
            metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();

            // fill in selected column meta
            for (int i = 1; i <= columnCount; ++i) {
                int kylinTypeId = dataSource.toKylinTypeId(metaData.getColumnTypeName(i), metaData.getColumnType(i));
                String kylinTypeName = dataSource.toKylinTypeName(kylinTypeId);
                returnColumnMeta.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i),
                        false, metaData.isCurrency(i), metaData.isNullable(i), false, metaData.getColumnDisplaySize(i),
                        metaData.getColumnLabel(i), metaData.getColumnName(i), null, null, null,
                        metaData.getPrecision(i), metaData.getScale(i), kylinTypeId, kylinTypeName,
                        metaData.isReadOnly(i), false, false));
            }
        } finally {
            DBUtils.closeQuietly(rs);
        }

    }

    @Override
    public void executeUpdate(String sql, String project) throws SQLException {
        dataSource.executeUpdate(sql);
    }

    @Override
    public String getName() {
        return QueryContext.PUSHDOWN_RDBMS;
    }

    private void extractResults(ResultSet resultSet, List<List<String>> results) throws SQLException {
        List<String> oneRow = new LinkedList<>();

        while (resultSet.next()) {
            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                oneRow.add((resultSet.getString(i + 1)));
            }

            results.add(new LinkedList<>(oneRow));
            oneRow.clear();
        }
    }
}
