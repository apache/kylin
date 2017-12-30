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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableExtDesc.ColumnStats;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.hive.HiveMRInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcHiveMRInput extends HiveMRInput {

    private static final Logger logger = LoggerFactory.getLogger(JdbcHiveMRInput.class);

    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new BatchCubingInputSide(flatDesc);
    }

    public static class BatchCubingInputSide extends HiveMRInput.BatchCubingInputSide {

        public BatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            super(flatDesc);
        }
        
        private KylinConfig getConfig() {
            return flatDesc.getDataModel().getConfig();
        }

        @Override
        protected void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow);

            jobFlow.addTask(createSqoopToFlatHiveStep(jobWorkingDir, cubeName));
            jobFlow.addTask(createFlatHiveTableFromFiles(hiveInitStatements, jobWorkingDir));
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
        private TblColRef determineSplitColumn() {
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
                List<ColumnStats> columnStatses = tableExtDesc.getColumnStats();
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

        private AbstractExecutable createSqoopToFlatHiveStep(String jobWorkingDir, String cubeName) {
            KylinConfig config = getConfig();
            PartitionDesc partitionDesc = flatDesc.getDataModel().getPartitionDesc();
            String partCol = null;
            String partitionString = null;

            if (partitionDesc.isPartitioned()) {
                partCol = partitionDesc.getPartitionDateColumn();//tablename.colname
                partitionString = partitionDesc.getPartitionConditionBuilder().buildDateRangeCondition(partitionDesc,
                        flatDesc.getSegment(), flatDesc.getSegRange());
            }

            String splitTable;
            String splitColumn;
            String splitDatabase;
            TblColRef splitColRef = determineSplitColumn();
            splitTable = splitColRef.getTableRef().getTableName();
            splitColumn = splitColRef.getName();
            splitDatabase = splitColRef.getColumnDesc().getTable().getDatabase();

            //using sqoop to extract data from jdbc source and dump them to hive
            String selectSql = JoinedFlatTable.generateSelectDataStatement(flatDesc, true, new String[] { partCol });
            String hiveTable = flatDesc.getTableName();
            String connectionUrl = config.getJdbcSourceConnectionUrl();
            String driverClass = config.getJdbcSourceDriver();
            String jdbcUser = config.getJdbcSourceUser();
            String jdbcPass = config.getJdbcSourcePass();
            String sqoopHome = config.getSqoopHome();
            String filedDelimiter = config.getJdbcSourceFieldDelimiter();
            int mapperNum = config.getSqoopMapperNum();

            String bquery = String.format("SELECT min(%s), max(%s) FROM %s.%s", splitColumn, splitColumn, splitDatabase,
                    splitTable);
            if (partitionString != null) {
                bquery += " WHERE " + partitionString;
            }

            String cmd = String.format(String.format(
                    "%s/sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true "
                            + "--connect \"%s\" --driver %s --username %s --password %s --query \"%s AND \\$CONDITIONS\" "
                            + "--target-dir %s/%s --split-by %s.%s --boundary-query \"%s\" --null-string '' "
                            + "--fields-terminated-by '%s' --num-mappers %d",
                    sqoopHome, connectionUrl, driverClass, jdbcUser, jdbcPass, selectSql, jobWorkingDir, hiveTable,
                    splitTable, splitColumn, bquery, filedDelimiter, mapperNum));
            logger.debug(String.format("sqoop cmd:%s", cmd));
            CmdStep step = new CmdStep();
            step.setCmd(cmd);
            step.setName(ExecutableConstants.STEP_NAME_SQOOP_TO_FLAT_HIVE_TABLE);
            return step;
        }

        @Override
        protected void addStepPhase1_DoMaterializeLookupTable(DefaultChainedExecutable jobFlow) {
            // skip
        }
    }
}
