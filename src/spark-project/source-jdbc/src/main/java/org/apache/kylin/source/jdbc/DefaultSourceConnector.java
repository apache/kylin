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
package org.apache.kylin.source.jdbc;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.sdk.datasource.adaptor.AdaptorConfig;
import org.apache.kylin.sdk.datasource.adaptor.DefaultAdaptor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// for reflection
public class DefaultSourceConnector extends DefaultAdaptor implements ISourceConnector {

    public DefaultSourceConnector(AdaptorConfig config) throws Exception {
        super(config);
    }

    // Reflected by JdbcSourceInput#getSourceData only, do not abuse it!
    public DefaultSourceConnector() {
        super();
    }

    @Override
    public Dataset<Row> getSourceData(KylinConfig kylinConfig, SparkSession sparkSession, String sql,
            Map<String, String> params) {
        String url = kylinConfig.getJdbcConnectionUrl();
        String user = kylinConfig.getJdbcUser();
        String password = kylinConfig.getJdbcPass();
        String driver = kylinConfig.getJdbcDriver();
        return sparkSession.read().format("jdbc").option("url", url).option("user", user).option("password", password)
                .option("driver", driver).option("query", sql).options(params).load();
    }

    @Override
    public Dataset<Row> getCountData(KylinConfig kylinConfig, SparkSession sparkSession, String sql,
            Map<String, String> params) {
        return getSourceData(kylinConfig, sparkSession, sql, params);
    }

}
