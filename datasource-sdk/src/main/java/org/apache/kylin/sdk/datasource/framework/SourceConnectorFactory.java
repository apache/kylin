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
package org.apache.kylin.sdk.datasource.framework;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.sdk.datasource.adaptor.AdaptorConfig;
import org.apache.kylin.sdk.datasource.adaptor.DefaultAdaptor;
import org.apache.kylin.sdk.datasource.adaptor.MysqlAdaptor;

public class SourceConnectorFactory {
    public static JdbcConnector getJdbcConnector(KylinConfig config) {
        String jdbcUrl = config.getJdbcSourceConnectionUrl();
        String jdbcDriver = config.getJdbcSourceDriver();
        String jdbcUser = config.getJdbcSourceUser();
        String jdbcPass = config.getJdbcSourcePass();
        String adaptorClazz = config.getJdbcSourceAdaptor();

        AdaptorConfig jdbcConf = new AdaptorConfig(jdbcUrl, jdbcDriver, jdbcUser, jdbcPass);
        jdbcConf.poolMaxIdle = config.getPoolMaxIdle();
        jdbcConf.poolMinIdle = config.getPoolMinIdle();
        jdbcConf.poolMaxTotal = config.getPoolMaxTotal();
        jdbcConf.datasourceId = config.getJdbcSourceDialect();

        if (adaptorClazz == null)
            adaptorClazz = decideAdaptorClassName(jdbcConf.datasourceId);

        try {
            return new JdbcConnector(AdaptorFactory.createJdbcAdaptor(adaptorClazz, jdbcConf));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get JdbcConnector from env.", e);
        }
    }

    private static String decideAdaptorClassName(String dataSourceId) {
        switch (dataSourceId) {
        case "mysql":
            return MysqlAdaptor.class.getName();
        default:
            return DefaultAdaptor.class.getName();
        }
    }
}
