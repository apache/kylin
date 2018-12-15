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

import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SourceConfigurationUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.util.FlatTableSqlQuoteUtils;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.hive.HiveInputBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class JdbcHiveInputBase extends HiveInputBase {
    private static final Logger logger = LoggerFactory.getLogger(JdbcHiveInputBase.class);
    private static final String MR_OVERRIDE_QUEUE_KEY = "mapreduce.job.queuename";
    private static final String DEFAULT_QUEUE = "default";

    public static class JdbcBaseBatchCubingInputSide extends BaseBatchCubingInputSide {

        public JdbcBaseBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            super(flatDesc);
        }

        protected KylinConfig getConfig() {
            return flatDesc.getDataModel().getConfig();
        }

        @Override
        protected void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            jobFlow.addTask(createSqoopToFlatHiveStep(jobWorkingDir, cubeName));
            jobFlow.addTask(createFlatHiveTableFromFiles(hiveInitStatements, jobWorkingDir));
        }

        @Override
        protected void addStepPhase1_DoMaterializeLookupTable(DefaultChainedExecutable jobFlow) {
            // skip
        }

        private AbstractExecutable createFlatHiveTableFromFiles(String hiveInitStatements, String jobWorkingDir) {
            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
            String filedDelimiter = getConfig().getJdbcSourceFieldDelimiter();
            // Sqoop does not support exporting SEQUENSEFILE to Hive now SQOOP-869
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, jobWorkingDir,
                    "TEXTFILE", filedDelimiter);

            HiveCmdStep step = new HiveCmdStep();
            step.setCmd(hiveInitStatements + dropTableHql + createTableHql);
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            return step;
        }

        /**
         * Choose a better split-by column for sqoop. The strategy is:
         * 1. Prefer ClusteredBy column
         * 2. Prefer DistributedBy column
         * 3. Prefer Partition date column
         * 4. Prefer Higher cardinality column
         * 5. Prefer numeric column
         * 6. Pick a column at first glance
         * @return A column reference <code>TblColRef</code>for sqoop split-by
         */
        protected TblColRef determineSplitColumn() {
            if (null != flatDesc.getClusterBy()) {
                return flatDesc.getClusterBy();
            }
            if (null != flatDesc.getDistributedBy()) {
                return flatDesc.getDistributedBy();
            }
            PartitionDesc partitionDesc = flatDesc.getDataModel().getPartitionDesc();
            if (partitionDesc.isPartitioned()) {
                return partitionDesc.getPartitionDateColumnRef();
            }
            TblColRef splitColumn = null;
            TableMetadataManager tblManager = TableMetadataManager.getInstance(getConfig());
            long maxCardinality = 0;
            for (TableRef tableRef : flatDesc.getDataModel().getAllTables()) {
                TableExtDesc tableExtDesc = tblManager.getTableExt(tableRef.getTableDesc());
                List<TableExtDesc.ColumnStats> columnStatses = tableExtDesc.getColumnStats();
                if (!columnStatses.isEmpty()) {
                    for (TblColRef colRef : tableRef.getColumns()) {
                        long cardinality = columnStatses.get(colRef.getColumnDesc().getZeroBasedIndex())
                                .getCardinality();
                        splitColumn = cardinality > maxCardinality ? colRef : splitColumn;
                    }
                }
            }
            if (null == splitColumn) {
                for (TblColRef colRef : flatDesc.getAllColumns()) {
                    if (colRef.getType().isIntegerFamily()) {
                        return colRef;
                    }
                }
                splitColumn = flatDesc.getAllColumns().get(0);
            }

            return splitColumn;
        }

        private String getSqoopJobQueueName(KylinConfig config) {
            Map<String, String> mrConfigOverride = config.getMRConfigOverride();
            if (mrConfigOverride.containsKey(MR_OVERRIDE_QUEUE_KEY)) {
                return mrConfigOverride.get(MR_OVERRIDE_QUEUE_KEY);
            }
            return DEFAULT_QUEUE;
        }

        protected AbstractExecutable createSqoopToFlatHiveStep(String jobWorkingDir, String cubeName) {
            KylinConfig config = getConfig();
            PartitionDesc partitionDesc = flatDesc.getDataModel().getPartitionDesc();
            String partCol = null;

            if (partitionDesc.isPartitioned()) {
                partCol = partitionDesc.getPartitionDateColumn();//tablename.colname
            }

            String splitTable;
            String splitTableAlias;
            String splitColumn;
            String splitDatabase;
            TblColRef splitColRef = determineSplitColumn();
            splitTable = splitColRef.getTableRef().getTableName();
            splitTableAlias = splitColRef.getTableAlias();
            splitColumn = JoinedFlatTable.getQuotedColExpressionInSourceDB(flatDesc, splitColRef);
            splitDatabase = splitColRef.getColumnDesc().getTable().getDatabase();

            //using sqoop to extract data from jdbc source and dump them to hive
            String selectSql = JoinedFlatTable.generateSelectDataStatement(flatDesc, true, new String[] { partCol });
            selectSql = escapeQuotationInSql(selectSql);



            String hiveTable = flatDesc.getTableName();
            String connectionUrl = config.getJdbcSourceConnectionUrl();
            String driverClass = config.getJdbcSourceDriver();
            String jdbcUser = config.getJdbcSourceUser();
            String jdbcPass = config.getJdbcSourcePass();
            String sqoopHome = config.getSqoopHome();
            String filedDelimiter = config.getJdbcSourceFieldDelimiter();
            int mapperNum = config.getSqoopMapperNum();

            String bquery = String.format(Locale.ROOT, "SELECT min(%s), max(%s) FROM %s.%s as %s", splitColumn,
                    splitColumn, splitDatabase, splitTable, splitTableAlias);
            if (partitionDesc.isPartitioned()) {
                SegmentRange segRange = flatDesc.getSegRange();
                if (segRange != null && !segRange.isInfinite()) {
                    if (partitionDesc.getPartitionDateColumnRef().getTableAlias().equals(splitTableAlias)
                            && (partitionDesc.getPartitionTimeColumnRef() == null || partitionDesc
                            .getPartitionTimeColumnRef().getTableAlias().equals(splitTableAlias))) {
                        String quotedPartCond = FlatTableSqlQuoteUtils.quoteIdentifierInSqlExpr(flatDesc,
                                partitionDesc.getPartitionConditionBuilder().buildDateRangeCondition(partitionDesc,
                                        flatDesc.getSegment(), segRange),
                                "`");
                        bquery += " WHERE " + quotedPartCond;
                    }
                }
            }
            bquery = escapeQuotationInSql(bquery);

            // escape ` in cmd
            splitColumn = escapeQuotationInSql(splitColumn);

            String cmd = String.format(Locale.ROOT,
                    "%s/bin/sqoop import" + generateSqoopConfigArgString()
                            + "--connect \"%s\" --driver %s --username %s --password %s --query \"%s AND \\$CONDITIONS\" "
                            + "--target-dir %s/%s --split-by %s --boundary-query \"%s\" --null-string '' "
                            + "--fields-terminated-by '%s' --num-mappers %d",
                    sqoopHome, connectionUrl, driverClass, jdbcUser, jdbcPass, selectSql, jobWorkingDir, hiveTable,
                    splitColumn, bquery, filedDelimiter, mapperNum);
            logger.debug(String.format(Locale.ROOT, "sqoop cmd:%s", cmd));
            CmdStep step = new CmdStep();
            step.setCmd(cmd);
            step.setName(ExecutableConstants.STEP_NAME_SQOOP_TO_FLAT_HIVE_TABLE);
            return step;
        }

        protected String generateSqoopConfigArgString() {
            KylinConfig kylinConfig = getConfig();
            Map<String, String> config = Maps.newHashMap();
            config.put("mapreduce.job.queuename", getSqoopJobQueueName(kylinConfig)); // override job queue from mapreduce config
            config.putAll(SourceConfigurationUtil.loadSqoopConfiguration());
            config.putAll(kylinConfig.getSqoopConfigOverride());

            StringBuilder args = new StringBuilder(" -Dorg.apache.sqoop.splitter.allow_text_splitter=true ");
            for (Map.Entry<String, String> entry : config.entrySet()) {
                args.append(" -D" + entry.getKey() + "=" + entry.getValue() + " ");
            }
            return args.toString();
        }
    }

    protected static String escapeQuotationInSql(String sqlExpr) {
        sqlExpr = sqlExpr.replaceAll("\"", "\\\\\"");
        sqlExpr = sqlExpr.replaceAll("`", "\\\\`");
        return sqlExpr;
    }
}
