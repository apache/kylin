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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.util.Objects;
import java.util.Set;
import java.util.Locale;
import java.util.Collections;

import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.IInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.SparkCreatingFlatTable;
import org.apache.kylin.engine.spark.SparkExecutable;
import org.apache.kylin.engine.spark.SparkExecutableFactory;
import org.apache.kylin.engine.spark.SparkSqlBatch;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.util.FlatTableSqlQuoteUtils;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class HiveInputBase {

    private static final Logger logger = LoggerFactory.getLogger(HiveInputBase.class);

    public static class BaseBatchCubingInputSide implements IInput.IBatchCubingInputSide {

        final protected IJoinedFlatTableDesc flatDesc;
        final protected String flatTableDatabase;
        final protected String hdfsWorkingDir;

        List<String> hiveViewIntermediateTables = Lists.newArrayList();

        public BaseBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            this.flatDesc = flatDesc;
            this.flatTableDatabase = config.getHiveDatabaseForIntermediateTable();
            this.hdfsWorkingDir = config.getHdfsWorkingDirectory();
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
            final KylinConfig cubeConfig = cubeInstance.getConfig();

            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);

            // create flat table first
            addStepPhase1_DoCreateFlatTable(jobFlow);

            // create hive global dictionary
            KylinConfig dictConfig = flatDesc.getSegment().getConfig();
            String[] mrHiveDictColumns = dictConfig.getMrHiveDictColumnsExcludeRefColumns();
            if (Objects.nonNull(mrHiveDictColumns) && mrHiveDictColumns.length > 0
                    && !"".equals(mrHiveDictColumns[0])) {
                addStepPhase1_DoCreateMrHiveGlobalDict(jobFlow, mrHiveDictColumns);
            }

            // then count and redistribute
            if (cubeConfig.isHiveRedistributeEnabled()) {
                final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                //jobFlow.addTask(createRedistributeFlatHiveTableStep(hiveInitStatements, cubeName, flatDesc, cubeInstance.getDescriptor()));
                if (kylinConfig.isLivyEnabled() && cubeInstance.getEngineType() == IEngineAware.ID_SPARK) {
                    jobFlow.addTask(createRedistributeFlatHiveTableByLivyStep(hiveInitStatements, cubeName, flatDesc,
                            cubeInstance.getDescriptor()));
                } else {
                    jobFlow.addTask(createRedistributeFlatHiveTableStep(hiveInitStatements, cubeName, flatDesc,
                            cubeInstance.getDescriptor()));
                }
            }

            // special for hive
            addStepPhase1_DoMaterializeLookupTable(jobFlow);
        }

        @Override
        public void addStepPhase_ReplaceFlatTableGlobalColumnValue(DefaultChainedExecutable jobFlow) {
            KylinConfig dictConfig = flatDesc.getSegment().getConfig();
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            String globalDictTable = MRHiveDictUtil.globalDictTableName(flatDesc, cubeName);
            String globalDictDatabase = dictConfig.getMrHiveDictDB();

            String[] mrHiveDictColumnsExcludeRefCols = dictConfig.getMrHiveDictColumnsExcludeRefColumns();
            Map<String, String> dictRef = dictConfig.getMrHiveDictRefColumns();
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);

            if (Objects.nonNull(mrHiveDictColumnsExcludeRefCols) && mrHiveDictColumnsExcludeRefCols.length > 0) {
                jobFlow.addTask(createHiveGlobalDictMergeGlobalDict(flatDesc, hiveInitStatements, cubeName, mrHiveDictColumnsExcludeRefCols, globalDictDatabase, globalDictTable));
                for (String item : mrHiveDictColumnsExcludeRefCols) {
                    dictRef.put(item, "");
                }
            }

            // replace step
            if (!dictRef.isEmpty()) {
                jobFlow.addTask(createMrHiveGlobalDictReplaceStep(flatDesc, hiveInitStatements, cubeName,
                        dictRef, flatTableDatabase, globalDictDatabase, globalDictTable, dictConfig.getMrHiveDictTableSuffix(), jobFlow.getId()));
            }
        }

        /**
         * 1. Create three related tables
         * 2. Insert distinct value into distinct value table
         * 3. Calculate statistics for dictionary
         */
        protected void addStepPhase1_DoCreateMrHiveGlobalDict(DefaultChainedExecutable jobFlow, String[] mrHiveDictColumns) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);

            jobFlow.addTask(createMrHiveGlobalDictExtractStep(flatDesc, hiveInitStatements, cubeName,
                    mrHiveDictColumns, jobFlow.getId()));

        }

        protected static AbstractExecutable createMrHiveGlobalDictExtractStep(IJoinedFlatTableDesc flatDesc, String hiveInitStatements,
                                                                              String cubeName, String[] mrHiveDictColumns, String jobId) {
            KylinConfig cfg = flatDesc.getSegment().getConfig();
            String globalDictTable = MRHiveDictUtil.globalDictTableName(flatDesc, cubeName);
            String globalDictDatabase = cfg.getMrHiveDictDB();
            final String distinctValueTable = MRHiveDictUtil.distinctValueTable(flatDesc);
            final String segmentLevelDictTableName = MRHiveDictUtil.segmentLevelDictTableName(flatDesc);

            final String createGlobalDictTableHql = MRHiveDictUtil.generateDictionaryDdl(globalDictDatabase, globalDictTable);
            final String dropDistinctValueTableHql = MRHiveDictUtil.generateDropTableStatement(distinctValueTable);
            final String createDistinctValueTableHql = MRHiveDictUtil.generateDistinctValueTableStatement(flatDesc);
            final String dropSegmentLevelDictTableHql = MRHiveDictUtil.generateDropTableStatement(segmentLevelDictTableName);
            final String createSegmentLevelDictTableHql = MRHiveDictUtil.generateDictTableStatement(segmentLevelDictTableName);

            String maxAndDistinctCountSql = MRHiveDictUtil.generateDictStatisticsSql(distinctValueTable, globalDictTable, globalDictDatabase);

            StringBuilder insertDataToDictIntermediateTableSql = new StringBuilder();
            for (String dictColumn : mrHiveDictColumns) {
                insertDataToDictIntermediateTableSql
                        .append(MRHiveDictUtil.generateInsertDataStatement(flatDesc, dictColumn, globalDictDatabase, globalDictTable));
            }
            String setParametersHql = "set hive.exec.compress.output=false;set hive.mapred.mode=unstrict;";
            CreateMrHiveDictStep step = new CreateMrHiveDictStep();
            step.setInitStatement(hiveInitStatements);
            step.setCreateTableStatement(setParametersHql
                    + createGlobalDictTableHql
                    + dropDistinctValueTableHql
                    + createDistinctValueTableHql
                    + dropSegmentLevelDictTableHql
                    + createSegmentLevelDictTableHql
                    + insertDataToDictIntermediateTableSql.toString()
                    + maxAndDistinctCountSql);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_GLOBAL_DICT_MRHIVE_EXTRACT_DICTVAL);
            step.setIsLock(true);
            step.setIsUnLock(false);
            step.setLockPathName(cubeName);
            step.setJobFlowJobId(jobId);
            return step;
        }

        /**
         * In the previous step, data of hive global dictionary is prepared by MR,
         * so now it is time for create partition for Segment Dictionary Table
         * and merge into Hive Global Dictionary Table.
         */
        protected static AbstractExecutable createHiveGlobalDictMergeGlobalDict(IJoinedFlatTableDesc flatDesc,
                                                                                String hiveInitStatements, String cubeName, String[] mrHiveDictColumns,
                                                                                String globalDictDatabase, String globalDictTable) {

            String globalDictIntermediateTable = MRHiveDictUtil.segmentLevelDictTableName(flatDesc);
            StringBuilder addPartitionHql = new StringBuilder();

            Map<String, String> dictHqlMap = new HashMap<>();
            for (String dictColumn : mrHiveDictColumns) {
                try {
                    addPartitionHql.append("ALTER TABLE ")
                            .append(globalDictIntermediateTable)
                            .append(" ADD IF NOT EXISTS PARTITION (dict_column='")
                            .append(dictColumn)
                            .append("');")
                            .append(" \n");

                    String dictHql = "INSERT OVERWRITE TABLE " + globalDictDatabase + "." + globalDictTable + " \n"
                            + "PARTITION (dict_column = '" + dictColumn + "') \n"
                            + "SELECT dict_key, dict_val FROM "
                            + globalDictDatabase + "." + globalDictTable + " \n" + "WHERE dict_column = '" + dictColumn
                            + "' \n" + flatDesc.getDataModel().getConfig().getHiveUnionStyle() + " \n"
                            + "SELECT dict_key, dict_val FROM "
                            + globalDictIntermediateTable + " \n" + " WHERE dict_column = '" + dictColumn + "' ;\n";
                    dictHqlMap.put(dictColumn, dictHql);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            String hiveInitStatementForUnstrict = "set hive.mapred.mode=unstrict;";
            CreateMrHiveDictStep step = new CreateMrHiveDictStep();
            step.setInitStatement(hiveInitStatements + hiveInitStatementForUnstrict + addPartitionHql);
            step.setCreateTableStatementMap(dictHqlMap);
            step.setIsLock(false);
            step.setIsUnLock(false);
            step.setLockPathName(cubeName);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_GLOBAL_DICT_MRHIVE_BUILD_DICTVAL);
            return step;
        }

        /**
         * Use Hive Global Dictionary to replace/encode flat table
         *
         * @param mrHiveDictColumns a Map which key is and vale is .
         */
        protected static AbstractExecutable createMrHiveGlobalDictReplaceStep(IJoinedFlatTableDesc flatDesc, String hiveInitStatements, String cubeName, Map<String, String> mrHiveDictColumns, String flatTableDatabase, String globalDictDatabase, String globalDictTable, String dictSuffix, String jobId) {
            Map<String, String> dictHqlMap = new HashMap<>();
            for (String dictColumn : mrHiveDictColumns.keySet()) {
                StringBuilder insertOverwriteHql = new StringBuilder();
                TblColRef dictColumnRef = null;

                String flatTable = flatTableDatabase + "." + flatDesc.getTableName();
                insertOverwriteHql.append("INSERT OVERWRITE TABLE ").append(flatTable).append(" \n");
                try {
                    insertOverwriteHql.append("SELECT \n");
                    int flatTableColumnSize = flatDesc.getAllColumns().size();
                    for (int i = 0; i < flatTableColumnSize; i++) {
                        TblColRef tblColRef = flatDesc.getAllColumns().get(i);
                        String colName = JoinedFlatTable.colName(tblColRef, flatDesc.useAlias());

                        if (i > 0) {
                            insertOverwriteHql.append(",");
                        }

                        if (colName.equalsIgnoreCase(dictColumn)) {
                            // Note: replace original value into encoded integer
                            insertOverwriteHql.append("b.dict_val \n");
                            dictColumnRef = tblColRef;
                        } else {
                            // Note: keep its original value
                            insertOverwriteHql.append("a.")
                                    .append(JoinedFlatTable.colName(tblColRef))
                                    .append(" \n");
                        }
                    }

                    if (!Strings.isNullOrEmpty(mrHiveDictColumns.get(dictColumn))) {
                        // Note: reuse previous hive global dictionary
                        String[] tableColumn = mrHiveDictColumns.get(dictColumn).split("\\.");

                        String refGlobalDictTable = tableColumn[0] + dictSuffix;
                        String refDictColumn = tableColumn[1];

                        insertOverwriteHql
                                .append("FROM ").append(flatTable).append(" a \nLEFT OUTER JOIN \n (")
                                .append("SELECT dict_key, dict_val FROM ")
                                .append(globalDictDatabase).append(".").append(refGlobalDictTable)
                                .append(" WHERE dict_column = '").append(refDictColumn).append("') b \n")
                                .append("ON a.").append(JoinedFlatTable.colName(dictColumnRef)).append(" = b.dict_key;");
                        dictHqlMap.put(dictColumn, insertOverwriteHql.toString());
                    } else {
                        // Note: use hive global dictionary built by current cube
                        insertOverwriteHql
                                .append("FROM ").append(flatTable).append(" a \nLEFT OUTER JOIN \n (")
                                .append("SELECT dict_key, dict_val FROM ")
                                .append(globalDictDatabase).append(".").append(globalDictTable)
                                .append(" WHERE dict_column = '").append(dictColumn).append("') b \n")
                                .append("ON a.").append(JoinedFlatTable.colName(dictColumnRef)).append(" = b.dict_key;");
                    }
                    dictHqlMap.put(dictColumn, insertOverwriteHql.toString());
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            String setParameterHal = "set hive.exec.compress.output=false; set hive.mapred.mode=unstrict;";
            CreateMrHiveDictStep step = new CreateMrHiveDictStep();
            step.setInitStatement(hiveInitStatements + setParameterHal);
            step.setCreateTableStatementMap(dictHqlMap);

            step.setIsUnLock(true);
            step.setLockPathName(cubeName);
            step.setJobFlowJobId(jobId);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_GLOBAL_DICT_MRHIVE_REPLACE_DICTVAL);
            return step;
        }

        protected void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);

            if (cubeInstance.getEngineType() == IEngineAware.ID_SPARK) {
                if (kylinConfig.isLivyEnabled()) {
                    jobFlow.addTask(createFlatHiveTableByLivyStep(hiveInitStatements,
                            jobWorkingDir, cubeName, flatDesc));
                } else {
                    if (kylinConfig.isSparCreateHiveTableViaSparkEnable()) {
                        jobFlow.addTask(createFlatHiveTableBySparkSql(hiveInitStatements,
                                jobWorkingDir, cubeName, flatDesc));
                    } else {
                        jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, flatDesc));
                    }
                }
            } else {
                jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, flatDesc));
            }
            //jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, flatDesc));
        }

        protected void addStepPhase1_DoMaterializeLookupTable(DefaultChainedExecutable jobFlow) {
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            AbstractExecutable task = createLookupHiveViewMaterializationStep(hiveInitStatements, jobWorkingDir,
                    flatDesc, hiveViewIntermediateTables, jobFlow.getId());
            if (task != null) {
                jobFlow.addTask(task);
            }
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            org.apache.kylin.source.hive.GarbageCollectionStep step = new org.apache.kylin.source.hive.GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_HIVE_CLEANUP);

            List<String> deleteTables = new ArrayList<>();
            deleteTables.add(getIntermediateTableIdentity());

            // mr-hive dict and inner table do not need delete hdfs
            String[] mrHiveDicts = flatDesc.getSegment().getConfig().getMrHiveDictColumns();
            if (Objects.nonNull(mrHiveDicts) && mrHiveDicts.length > 0) {
                String dictDb = flatDesc.getSegment().getConfig().getMrHiveDictDB();
                String tableName = dictDb + "." + flatDesc.getTableName() + "_"
                        + MRHiveDictUtil.DictHiveType.GroupBy.getName();
                deleteTables.add(tableName);
            }
            step.setIntermediateTables(deleteTables);

            step.setExternalDataPaths(Collections.singletonList(JoinedFlatTable.getTableDir(flatDesc, jobWorkingDir)));
            step.setHiveViewIntermediateTableIdentities(StringUtil.join(hiveViewIntermediateTables, ","));
            jobFlow.addTask(step);
        }

        protected String getIntermediateTableIdentity() {
            return flatTableDatabase + "." + flatDesc.getTableName();
        }
    }

    // ===== static methods ======

    protected static String getTableNameForHCat(TableDesc table, String uuid) {
        String tableName = (table.isView()) ? table.getMaterializedName(uuid) : table.getName();
        String database = (table.isView()) ? KylinConfig.getInstanceFromEnv().getHiveDatabaseForIntermediateTable()
                : table.getDatabase();
        return String.format(Locale.ROOT, "%s.%s", database, tableName).toUpperCase(Locale.ROOT);
    }

    protected static AbstractExecutable createFlatHiveTableStep(String hiveInitStatements, String jobWorkingDir,
                                                                String cubeName, IJoinedFlatTableDesc flatDesc) {
        //from hive to hive
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, jobWorkingDir);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);

        CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
        step.setInitStatement(hiveInitStatements);
        step.setCreateTableStatement(dropTableHql + createTableHql + insertDataHqls);
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static AbstractExecutable createFlatHiveTableByLivyStep(String hiveInitStatements, String jobWorkingDir,
                                                                      String cubeName, IJoinedFlatTableDesc flatDesc) {
        //from hive to hive
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, jobWorkingDir);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);

        CreateFlatHiveTableByLivyStep step = new CreateFlatHiveTableByLivyStep();
        step.setInitStatement(hiveInitStatements);
        step.setCreateTableStatement(dropTableHql + createTableHql + insertDataHqls);
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static AbstractExecutable createFlatHiveTableBySparkSql(String hiveInitStatements,
                                                                      String jobWorkingDir, String cubeName, IJoinedFlatTableDesc flatDesc) {
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc,
                jobWorkingDir);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);

        KylinConfig config = flatDesc.getSegment().getConfig();
        final SparkExecutable sparkExecutable = SparkExecutableFactory.instance(config);
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_TABLE_WITH_SPARK);
        sparkExecutable.setClassName(SparkCreatingFlatTable.class.getName());

        sparkExecutable.setParam(SparkSqlBatch.OPTION_CUBE_NAME.getOpt(), cubeName);
        sparkExecutable.setParam(SparkSqlBatch.OPTION_STEP_NAME.getOpt(),
                base64EncodeStr(ExecutableConstants.STEP_NAME_CREATE_FLAT_TABLE_WITH_SPARK));
        sparkExecutable.setParam(SparkSqlBatch.OPTION_SEGMENT_ID.getOpt(),
                flatDesc.getSegment().getName());
        sparkExecutable.setParam(SparkSqlBatch.OPTION_SQL_COUNT.getOpt(),
                String.valueOf(SparkCreatingFlatTable.SQL_COUNT));

        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(0).getOpt(),
                base64EncodeStr(hiveInitStatements));
        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(1).getOpt(),
                base64EncodeStr(dropTableHql));

        // createTableHql include create table sql and alter table sql
        String[] sqlArr = createTableHql.trim().split(";");
        if (2 != sqlArr.length) {
            throw new RuntimeException("create table hql should combined by a create table sql " +
                    "and a alter sql, but got: " + createTableHql);
        }
        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(2).getOpt(),
                base64EncodeStr(sqlArr[0]));
        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(3).getOpt(),
                base64EncodeStr(sqlArr[1]));

        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(4).getOpt(),
                base64EncodeStr(insertDataHqls));

        StringBuilder jars = new StringBuilder();
        StringUtil.appendWithSeparator(jars, config.getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());

        return sparkExecutable;
    }

    private static String base64EncodeStr(String str) {
        return new String(
                Base64.getEncoder().encode(str.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8
        );
    }

    protected static AbstractExecutable createRedistributeFlatHiveTableStep(String hiveInitStatements, String cubeName,
                                                                            IJoinedFlatTableDesc flatDesc, CubeDesc cubeDesc) {
        RedistributeFlatHiveTableStep step = new RedistributeFlatHiveTableStep();
        step.setInitStatement(hiveInitStatements);
        step.setIntermediateTable(flatDesc.getTableName());
        step.setRedistributeDataStatement(JoinedFlatTable.generateRedistributeFlatTableStatement(flatDesc, cubeDesc));
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static AbstractExecutable createRedistributeFlatHiveTableByLivyStep(String hiveInitStatements,
                                                                                  String cubeName, IJoinedFlatTableDesc flatDesc, CubeDesc cubeDesc) {
        RedistributeFlatHiveTableByLivyStep step = new RedistributeFlatHiveTableByLivyStep();
        step.setInitStatement(hiveInitStatements);
        step.setIntermediateTable(flatDesc.getTableName());
        step.setRedistributeDataStatement(JoinedFlatTable.generateRedistributeFlatTableStatement(flatDesc, cubeDesc));
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static ShellExecutable createLookupHiveViewMaterializationStep(String hiveInitStatements, String jobWorkingDir, IJoinedFlatTableDesc flatDesc,
                                                                             List<String> intermediateTables, String uuid) {
        ShellExecutable step = new ShellExecutable();
        step.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_HIVE_VIEW_IN_LOOKUP);

        KylinConfig kylinConfig = flatDesc.getSegment().getConfig();
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(kylinConfig);
        final Set<TableDesc> lookupViewsTables = Sets.newHashSet();

        String prj = flatDesc.getDataModel().getProject();
        for (JoinTableDesc lookupDesc : flatDesc.getDataModel().getJoinTables()) {
            TableDesc tableDesc = metadataManager.getTableDesc(lookupDesc.getTable(), prj);
            if (lookupDesc.getKind() == DataModelDesc.TableKind.LOOKUP && tableDesc.isView()) {
                lookupViewsTables.add(tableDesc);
            }
        }

        if (lookupViewsTables.size() == 0) {
            return null;
        }

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.overwriteHiveProps(kylinConfig.getHiveConfigOverride());
        hiveCmdBuilder.addStatement(hiveInitStatements);
        for (TableDesc lookUpTableDesc : lookupViewsTables) {
            String identity = FlatTableSqlQuoteUtils.quoteTableIdentity(lookUpTableDesc.getDatabase(), lookUpTableDesc.getName(), null);
            if (lookUpTableDesc.isView()) {
                String intermediate = lookUpTableDesc.getMaterializedName(uuid);
                String materializeViewHql = materializeViewHql(intermediate, identity, jobWorkingDir);
                hiveCmdBuilder.addStatement(materializeViewHql);
                intermediateTables.add(intermediate);
            }
        }

        step.setCmd(hiveCmdBuilder.build());
        return step;
    }

    // each append must be a complete hql.
    protected static String materializeViewHql(String viewName, String tableName, String jobWorkingDir) {
        StringBuilder createIntermediateTableHql = new StringBuilder();
        createIntermediateTableHql.append("DROP TABLE IF EXISTS `" + viewName + "`;\n");
        createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS `" + viewName + "` LIKE " + tableName
                + " LOCATION '" + jobWorkingDir + "/" + viewName + "';\n");
        createIntermediateTableHql.append("ALTER TABLE `" + viewName + "` SET TBLPROPERTIES('auto.purge'='true');\n");
        createIntermediateTableHql
                .append("INSERT OVERWRITE TABLE `" + viewName + "` SELECT * FROM " + tableName + ";\n");
        return createIntermediateTableHql.toString();
    }

    protected static String getJobWorkingDir(DefaultChainedExecutable jobFlow, String hdfsWorkingDir) {

        String jobWorkingDir = JobBuilderSupport.getJobWorkingDir(hdfsWorkingDir, jobFlow.getId());
        if (KylinConfig.getInstanceFromEnv().getHiveTableDirCreateFirst()) {
            // Create work dir to avoid hive create it,
            // the difference is that the owners are different.
            checkAndCreateWorkDir(jobWorkingDir);
        }
        return jobWorkingDir;
    }

    protected static void checkAndCreateWorkDir(String jobWorkingDir) {
        try {
            Path path = new Path(jobWorkingDir);
            FileSystem fileSystem = HadoopUtil.getFileSystem(path);
            if (!fileSystem.exists(path)) {
                logger.info("Create jobWorkDir : " + jobWorkingDir);
                fileSystem.mkdirs(path);
            }
        } catch (IOException e) {
            logger.error("Could not create lookUp table dir : " + jobWorkingDir);
        }
    }

}
