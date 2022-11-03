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

package org.apache.kylin.query.util;

import io.kyligence.kap.query.util.KapQueryUtil;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;


public class QueryHelper {

    private QueryHelper() {
    }

    static final String RUN_CONSTANT_QUERY_LOCALLY = "kylin.query.engine.run-constant-query-locally";

    // Expose this method for MDX, don't remove it.
    public static Dataset<Row> singleQuery(String sql, String project) throws SQLException {
        val prevRunLocalConf = Unsafe.setProperty(RUN_CONSTANT_QUERY_LOCALLY, "FALSE");
        try {
            val projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).getConfig();
            val queryExec = new QueryExec(project, projectKylinConfig);
            val queryParams = new QueryParams(KapQueryUtil.getKylinConfig(project),
                    sql, project, 0, 0, queryExec.getDefaultSchemaName(), true);
            val convertedSql = KapQueryUtil.massageSql(queryParams);
            queryExec.executeQuery(convertedSql);
        } finally {
            if (prevRunLocalConf == null) {
                Unsafe.clearProperty(RUN_CONSTANT_QUERY_LOCALLY);
            } else {
                Unsafe.setProperty(RUN_CONSTANT_QUERY_LOCALLY, prevRunLocalConf);
            }
        }
        return SparderEnv.getDF();
    }

    public static Dataset<Row> sql(SparkSession session, String project, String sqlText) {
        try {
            return singleQuery(sqlText, project);
        } catch (SQLException e) {
            return session.sql(sqlText);
        }
    }
}
