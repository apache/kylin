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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.LogicalViewLoader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JdbcSourceInput implements NSparkCubingEngine.NSparkCubingSource {
    @Override
    public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters) {
        if (table.getIdentity().contains(table.getConfig().getDDLLogicalViewDB())) {
            LogicalViewLoader.addCatalogConfByJdbc(ss, table.getProject());
            return SourceFactory.getSparkSource().adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class)
                    .getSourceData(table, ss, parameters);
        }
        ColumnDesc[] columnDescs = table.getColumns();
        List<String> tblColNames = Lists.newArrayListWithCapacity(columnDescs.length);
        StructType kylinSchema = new StructType();
        for (ColumnDesc columnDesc : columnDescs) {
            if (!columnDesc.isComputedColumn()) {
                kylinSchema = kylinSchema.add(columnDesc.getName(),
                        SparderTypeUtil.toSparkType(columnDesc.getType(), false), true);
                tblColNames.add(columnDesc.getName());
            }
        }
        String[] colNames = tblColNames.toArray(new String[0]);
        String colString = Joiner.on(",").join(colNames);
        String sql = String.format(Locale.ROOT, "select %s from %s", colString, table.getIdentity());
        KylinConfig config = table.getConfig();
        ISourceConnector connector = (ISourceConnector) ClassUtil.newInstance(config.getJdbcSourceConnector());
        parameters.put("table", table.getIdentity());
        log.info("Use source connector: {}", connector.getClass().getCanonicalName());
        Dataset<Row> dataset = connector.getSourceData(config, ss, sql, parameters);
        StructType sparkSchema = dataset.schema();
        return dataset.select(SparderTypeUtil.alignDataTypeAndName(sparkSchema, kylinSchema));
    }

    @Override
    public Long getSourceDataCount(TableDesc table, SparkSession ss, Map<String, String> parameters) {
        if (table.getIdentity().contains(table.getConfig().getDDLLogicalViewDB())) {
            LogicalViewLoader.addCatalogConfByJdbc(ss, table.getProject());
            return SourceFactory.getSparkSource().adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class)
                    .getSourceDataCount(table, ss, parameters);
        }
        String sql = String.format(Locale.ROOT, "select count(*) from %s", table.getIdentity());
        KylinConfig config = table.getConfig();
        ISourceConnector connector = (ISourceConnector) ClassUtil.newInstance(config.getJdbcSourceConnector());
        parameters.put("table", table.getIdentity());
        log.info("Use source connector: {}, sql: {} ", connector.getClass().getCanonicalName(), sql);
        Dataset<Row> dataset = connector.getCountData(config, ss, sql, parameters);
        return dataset.first().getLong(0);
    }

}
