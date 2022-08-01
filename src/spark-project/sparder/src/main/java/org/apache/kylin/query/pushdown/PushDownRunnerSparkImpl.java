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

import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.apache.kylin.metadata.query.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushDownRunnerSparkImpl implements IPushDownRunner {
    public static final Logger logger = LoggerFactory.getLogger(PushDownRunnerSparkImpl.class);

    @Override
    public void init(KylinConfig config) {
        // SparkSession has been initialized
    }

    @Override
    public void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas,
            String project) {
        PushdownResult response = executeQueryToIterator(query, project);
        response.getRows().forEach(results::add);
        columnMetas.addAll(response.getColumnMetas());
    }

    @Override
    public PushdownResult executeQueryToIterator(String query, String project) {
        PushdownResponse response = queryWithPushDown(query, project);
        int columnCount = response.getColumns().size();
        List<StructField> fieldList = response.getColumns();
        List<SelectedColumnMeta> columnMetas = new LinkedList<>();
        // fill in selected column meta
        for (int i = 0; i < columnCount; ++i) {
            int nullable = fieldList.get(i).isNullable() ? 1 : 0;
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, true, Integer.MAX_VALUE,
                    fieldList.get(i).getName(), fieldList.get(i).getName(), null, null, null,
                    fieldList.get(i).getPrecision(), fieldList.get(i).getScale(), fieldList.get(i).getDataType(),
                    fieldList.get(i).getDataTypeName(), false, false, false));
        }

        return new PushdownResult(response.getRows(), response.getSize(), columnMetas);
    }

    @Override
    public void executeUpdate(String sql, String project) {
        queryWithPushDown(sql, project);
    }

    private PushdownResponse queryWithPushDown(String sql, String project) {
        return SparkSubmitter.getInstance().submitPushDownTask(sql, project);
    }

    @Override
    public String getName() {
        return QueryContext.PUSHDOWN_HIVE;
    }
}
