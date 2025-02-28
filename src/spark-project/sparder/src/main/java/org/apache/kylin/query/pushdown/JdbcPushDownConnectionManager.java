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

package org.apache.kylin.query.pushdown;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class JdbcPushDownConnectionManager {

    private static final Map<String, JdbcPushDownConnectionManager> managerMap = Maps.newConcurrentMap();

    static JdbcPushDownConnectionManager getConnectionManager(KylinConfig config, String project)
            throws ClassNotFoundException {
        JdbcPushDownConnectionManager manager = managerMap.get(project);
        DataSourceConfig newDataSourceConfig = new DataSourceConfig(config);
        if (needUpdateProjectConnectionManager(manager, newDataSourceConfig)) {
            synchronized (JdbcPushDownConnectionManager.class) {
                if (needUpdateProjectConnectionManager(manager, newDataSourceConfig)) {
                    manager = new JdbcPushDownConnectionManager(config, newDataSourceConfig);
                    managerMap.put(project, manager);
                }
            }
        }
        return manager;
    }

    static boolean needUpdateProjectConnectionManager(JdbcPushDownConnectionManager manager, DataSourceConfig config) {
        return manager == null || !manager.dataSourceConfig.equals(config);
    }

    private final BasicDataSource dataSource;
    private final DataSourceConfig dataSourceConfig;

    private JdbcPushDownConnectionManager(KylinConfig config, DataSourceConfig newDataSourceConfig)
            throws ClassNotFoundException {
        dataSource = new BasicDataSource();
        dataSourceConfig = newDataSourceConfig;

        Class.forName(config.getJdbcDriverClass());
        dataSource.setDriverClassName(config.getJdbcDriverClass());
        dataSource.setUrl(config.getJdbcUrl());
        dataSource.setUsername(config.getJdbcUsername());
        dataSource.setPassword(config.getJdbcPassword());
        dataSource.setMaxTotal(config.getPoolMaxTotal());
        dataSource.setMaxIdle(config.getPoolMaxIdle());
        dataSource.setMinIdle(config.getPoolMinIdle());

        // Default settings
        dataSource.setTestOnBorrow(true);
        dataSource.setValidationQuery("select 1");
        dataSource.setRemoveAbandonedOnBorrow(true);
        dataSource.setRemoveAbandonedTimeout(300);
    }

    public void close() {
        try {
            dataSource.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close(Connection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
