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
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class HiveInputBase {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HiveInputBase.class);

    protected static String getTableNameForHCat(TableDesc table, String uuid) {
        String tableName = (table.isView()) ? table.getMaterializedName(uuid) : table.getName();
        String database = (table.isView()) ? KylinConfig.getInstanceFromEnv().getHiveDatabaseForIntermediateTable()
                : table.getDatabase();
        return String.format(Locale.ROOT, "%s.%s", database, tableName).toUpperCase(Locale.ROOT);
    }

    protected void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow, String hdfsWorkingDir,
            IJoinedFlatTableDesc flatTableDesc, String flatTableDatabase) {
        final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
        final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
        final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

        jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, flatTableDesc));
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

    protected static ShellExecutable createLookupHiveViewMaterializationStep(String hiveInitStatements,
            String jobWorkingDir, IJoinedFlatTableDesc flatDesc, List<String> intermediateTables, String uuid) {
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
            String identity = lookUpTableDesc.getIdentity();
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
        createIntermediateTableHql.append("DROP TABLE IF EXISTS " + viewName + ";\n");
        createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS " + viewName + " LIKE " + tableName
                + " LOCATION '" + jobWorkingDir + "/" + viewName + "';\n");
        createIntermediateTableHql.append("ALTER TABLE " + viewName + " SET TBLPROPERTIES('auto.purge'='true');\n");
        createIntermediateTableHql.append("INSERT OVERWRITE TABLE " + viewName + " SELECT * FROM " + tableName + ";\n");
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
