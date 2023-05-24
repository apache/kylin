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
package org.apache.kylin.engine.spark.source;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.utils.HiveTransactionTableHelper;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;
import static org.apache.kylin.engine.spark.stats.utils.HiveTableRefChecker.isNeedCreateHiveTemporaryTable;

public class NSparkCubingSourceInput implements NSparkCubingEngine.NSparkCubingSource {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingSourceInput.class);

    @Override
    public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> params) {
        KylinConfig kylinConfig = KylinBuildEnv.get().kylinConfig();
        logger.info("isRangePartition:{};isTransactional:{};isReadTransactionalTableEnabled:{}",
                table.isRangePartition(), table.isTransactional(), kylinConfig.isReadTransactionalTableEnabled());

        // Extract effective columns which column exists in both kylin and source table
        List<ColumnDesc> effectiveColumns = extractEffectiveColumns(table, ss);
        String sql = generateSelectSql(table, effectiveColumns, params, kylinConfig);
        StructType kylinSchema = generateKylinSchema(effectiveColumns);
        if (logger.isDebugEnabled()) {
            logger.debug("Source data sql is: {}", sql);
            logger.debug("Kylin schema: {}", kylinSchema.treeString());
        }
        Dataset<Row> df = ss.sql(sql);
        StructType sparkSchema = df.schema();

        // Caution: sparkSchema and kylinSchema should keep same column order before aligning
        return df.select(SparderTypeUtil.alignDataTypeAndName(sparkSchema, kylinSchema));
    }

    private List<ColumnDesc> extractEffectiveColumns(TableDesc table, SparkSession ss) {
        List<ColumnDesc> ret = new ArrayList<>();
        Dataset<Row> sourceTableDS = ss.table(table.getBackTickIdentity());
        Set<String> sourceTableColumns = Arrays.stream(sourceTableDS.columns()).map(String::toUpperCase)
                .collect(Collectors.toSet());
        for (ColumnDesc col : table.getColumns()) {
            if (!col.isComputedColumn()) {
                if (sourceTableColumns.contains(col.getName())) {
                    ret.add(col);
                } else {
                    logger.warn("Table {} missing column {} in source schema", table.getTableAlias(), col.getName());
                }
            }
        }
        return ret;
    }

    private String generateSelectSql(TableDesc table, List<ColumnDesc> effectiveColumns, Map<String, String> params, KylinConfig kylinConfig) {
        String colString = generateColString(effectiveColumns);
        String sql;
        if (isNeedCreateHiveTemporaryTable(table.isRangePartition(), table.isTransactional(),
                kylinConfig.isReadTransactionalTableEnabled())) {
            sql = HiveTransactionTableHelper.doGetQueryHiveTemporaryTableSql(table, params, colString,
                    KylinBuildEnv.get());
        } else {
            sql = String.format(Locale.ROOT, "select %s from %s", colString, table.getBackTickIdentity());
        }
        return sql;
    }

    private String generateColString(List<ColumnDesc> effectiveColumns) {
        return effectiveColumns.stream().map(col -> BACK_TICK.string + col.getName() + BACK_TICK.string)
                .collect(Collectors.joining(","));
    }

    private StructType generateKylinSchema(List<ColumnDesc> effectiveColumns) {
        StructType kylinSchema = new StructType();
        for (ColumnDesc columnDesc : effectiveColumns) {
            if (!columnDesc.isComputedColumn()) {
                kylinSchema = kylinSchema.add(columnDesc.getName(),
                        SparderTypeUtil.toSparkType(columnDesc.getType(), false), true);
            }
        }
        return kylinSchema;
    }
}
