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
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.IRealizationSegment;

public class HiveMRInput implements IMRInput {

    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(IRealizationSegment seg) {
        return new BatchCubingInputSide(seg);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table) {
        return new HiveTableInputFormat(table.getIdentity());
    }

    public static class HiveTableInputFormat implements IMRTableInputFormat {
        final String dbName;
        final String tableName;

        /**
         * Construct a HiveTableInputFormat to read hive table.
         * @param fullQualifiedTableName "databaseName.tableName"
         */
        public HiveTableInputFormat(String fullQualifiedTableName) {
            String[] parts = HadoopUtil.parseHiveTableName(fullQualifiedTableName);
            dbName = parts[0];
            tableName = parts[1];
        }

        @Override
        public void configureJob(Job job) {
            try {
                HCatInputFormat.setInput(job, dbName, tableName);
                job.setInputFormatClass(HCatInputFormat.class);

                job.setMapOutputValueClass(org.apache.hive.hcatalog.data.DefaultHCatRecord.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String[] parseMapperInput(Object mapperInput) {
            return HiveTableReader.getRowAsStringArray((HCatRecord) mapperInput);
        }

    }

    public static class BatchCubingInputSide implements IMRBatchCubingInputSide {

        final JobEngineConfig conf;
        final IRealizationSegment seg;
        final IJoinedFlatTableDesc flatHiveTableDesc;
        String hiveViewIntermediateTables = "";

        public BatchCubingInputSide(IRealizationSegment seg) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.seg = seg;
            this.flatHiveTableDesc = seg.getJoinedFlatTableDesc();
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());

            final String rowCountOutputDir = JobBuilderSupport.getJobWorkingDir(conf, jobFlow.getId()) + "/row_count";

            jobFlow.addTask(createCountHiveTableStep(conf, flatHiveTableDesc, jobFlow.getId(), rowCountOutputDir));
            jobFlow.addTask(createFlatHiveTableStep(conf, flatHiveTableDesc, jobFlow.getId(), cubeName, rowCountOutputDir));
            AbstractExecutable task = createLookupHiveViewMaterializationStep(jobFlow.getId());
            if(task != null) {
                jobFlow.addTask(task);
            }
        }

        public static AbstractExecutable createCountHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId, String rowCountOutputDir) {
            final ShellExecutable step = new ShellExecutable();

            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
            hiveCmdBuilder.addStatement(JoinedFlatTable.generateHiveSetStatements(conf));
            hiveCmdBuilder.addStatement("set hive.exec.compress.output=false;\n");
            hiveCmdBuilder.addStatement(JoinedFlatTable.generateCountDataStatement(flatTableDesc, rowCountOutputDir));

            step.setCmd(hiveCmdBuilder.build());
            step.setName(ExecutableConstants.STEP_NAME_COUNT_HIVE_TABLE);

            return step;
        }

        public ShellExecutable createLookupHiveViewMaterializationStep(String jobId) {
            ShellExecutable step = new ShellExecutable();;
            step.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_HIVE_VIEW_IN_LOOKUP);
            HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();

            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(kylinConfig);
            String cubeName = seg.getRealization().getName();
            CubeDesc cubeDesc = cubeMgr.getCube(cubeName).getDescriptor();
            MetadataManager metadataManager = MetadataManager.getInstance(kylinConfig);
            final Set<TableDesc> lookupViewsTables = Sets.newHashSet();

            for (LookupDesc lookupDesc : cubeDesc.getModel().getLookups()) {
                TableDesc tableDesc = metadataManager.getTableDesc(lookupDesc.getTable());
                if (TableDesc.TABLE_TYPE_VIRTUAL_VIEW.equalsIgnoreCase(tableDesc.getTableType())) {
                    lookupViewsTables.add(tableDesc);
                }
            }

            if(lookupViewsTables.size() == 0) {
                return null;
            }
            final String useDatabaseHql = "USE " + conf.getConfig().getHiveDatabaseForIntermediateTable() + ";";
            hiveCmdBuilder.addStatement(useDatabaseHql);
            hiveCmdBuilder.addStatement(JoinedFlatTable.generateHiveSetStatements(conf));
            for(TableDesc lookUpTableDesc : lookupViewsTables) {
                if (TableDesc.TABLE_TYPE_VIRTUAL_VIEW.equalsIgnoreCase(lookUpTableDesc.getTableType())) {
                    StringBuilder createIntermediateTableHql = new StringBuilder();
                    createIntermediateTableHql.append("DROP TABLE IF EXISTS " + lookUpTableDesc.getMaterializedName() + ";\n");
                    createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS " +
                            lookUpTableDesc.getMaterializedName() + "\n");
                    createIntermediateTableHql.append("LOCATION '" + JobBuilderSupport.getJobWorkingDir(conf, jobId) + "/" +
                            lookUpTableDesc.getMaterializedName() + "'\n");
                    createIntermediateTableHql.append("AS SELECT * FROM " + lookUpTableDesc.getIdentity() + ";\n");
                    hiveCmdBuilder.addStatement(createIntermediateTableHql.toString());
                    hiveViewIntermediateTables = hiveViewIntermediateTables + lookUpTableDesc.getMaterializedName() + ";";
                }
            }

            hiveViewIntermediateTables = hiveViewIntermediateTables.substring(0, hiveViewIntermediateTables.length() - 1);

            step.setCmd(hiveCmdBuilder.build());
            return step;
        }

        public static AbstractExecutable createFlatHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId, String cubeName, String rowCountOutputDir) {
            StringBuilder hiveInitBuf = new StringBuilder();
            hiveInitBuf.append(JoinedFlatTable.generateHiveSetStatements(conf));

            final String useDatabaseHql = "USE " + conf.getConfig().getHiveDatabaseForIntermediateTable() + ";";
            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
            String insertDataHqls;
            insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc, conf);

            CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
            step.setInitStatement(hiveInitBuf.toString());
            step.setRowCountOutputDir(rowCountOutputDir);
            step.setCreateTableStatement(useDatabaseHql + dropTableHql + createTableHql + insertDataHqls);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            return step;
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            GarbageCollectionStep step = new GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
            step.setIntermediateTableIdentity(getIntermediateTableIdentity());
            step.setExternalDataPath(JoinedFlatTable.getTableDir(flatHiveTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobFlow.getId())));
            step.setHiveViewIntermediateTableIdentities(hiveViewIntermediateTables);
            jobFlow.addTask(step);
        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new HiveTableInputFormat(getIntermediateTableIdentity());
        }

        private String getIntermediateTableIdentity() {
            return conf.getConfig().getHiveDatabaseForIntermediateTable() + "." + flatHiveTableDesc.getTableName();
        }
    }

    public static class GarbageCollectionStep extends AbstractExecutable {
        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            KylinConfig config = context.getConfig();
            StringBuffer output = new StringBuffer();
            try {
                output.append(cleanUpIntermediateFlatTable(config));
                // don't drop view to avoid concurrent issue
                //output.append(cleanUpHiveViewIntermediateTable(config));
            } catch (IOException e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                return new ExecuteResult(ExecuteResult.State.ERROR, e.getMessage());
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
        }

        private String cleanUpIntermediateFlatTable(KylinConfig config) throws IOException {
            StringBuffer output = new StringBuffer();
            final String hiveTable = this.getIntermediateTableIdentity();
            if (config.isHiveKeepFlatTable() == false && StringUtils.isNotEmpty(hiveTable)) {
                final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
                hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
                hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS  " + hiveTable + ";");

                config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
                output.append("Hive table " + hiveTable + " is dropped. \n");

                rmdirOnHDFS(getExternalDataPath());
                output.append("Hive table " + hiveTable + " external data path " + getExternalDataPath() + " is deleted. \n");
            }
            return output.toString();
        }

        private void mkdirOnHDFS(String path) throws IOException {
            Path externalDataPath = new Path(path);
            FileSystem fs = FileSystem.get(externalDataPath.toUri(), HadoopUtil.getCurrentConfiguration());
            if (!fs.exists(externalDataPath)) {
                fs.mkdirs(externalDataPath);
            }
        }

        private void rmdirOnHDFS(String path) throws IOException {
            Path externalDataPath = new Path(path);
            FileSystem fs = FileSystem.get(externalDataPath.toUri(), HadoopUtil.getCurrentConfiguration());
            if (fs.exists(externalDataPath)) {
                fs.delete(externalDataPath, true);
            }
        }

        private String cleanUpHiveViewIntermediateTable(KylinConfig config) throws IOException {
            StringBuffer output = new StringBuffer();
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
            hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
            if (getHiveViewIntermediateTableIdentities() != null && !getHiveViewIntermediateTableIdentities().isEmpty()) {
                for(String hiveTableName : getHiveViewIntermediateTableIdentities().split(";")) {
                    hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS  " + hiveTableName + ";");
                }
            }
            config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
            output.append("hive view intermediate tables: " + getHiveViewIntermediateTableIdentities() + " is dropped. \n");
            return output.toString();
        }

        public void setIntermediateTableIdentity(String tableIdentity) {
            setParam("oldHiveTable", tableIdentity);
        }

        private String getIntermediateTableIdentity() {
            return getParam("oldHiveTable");
        }

        public void setExternalDataPath(String externalDataPath) {
            setParam("externalDataPath", externalDataPath);
        }

        private String getExternalDataPath() {
            return getParam("externalDataPath");
        }

        public void setHiveViewIntermediateTableIdentities(String tableIdentities) {
            setParam("oldHiveViewIntermediateTables", tableIdentities);
        }

        private String getHiveViewIntermediateTableIdentities() {
            return getParam("oldHiveViewIntermediateTables");
        }
    }

}
