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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIJoinedFlatTableDesc;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
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

        public BatchCubingInputSide(IRealizationSegment seg) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.seg = seg;
            this.flatHiveTableDesc = seg.getJoinedFlatTableDesc();
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createFlatHiveTableStep(conf, flatHiveTableDesc, jobFlow.getId()));
        }

        public static AbstractExecutable createFlatHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId) {

            final String useDatabaseHql = "USE " + conf.getConfig().getHiveDatabaseForIntermediateTable() + ";";
            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
            String insertDataHqls;
            try {
                insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc, conf);
            } catch (IOException e) {
                throw new RuntimeException("Failed to generate insert data SQL for intermediate table.", e);
            }

            ShellExecutable step = new ShellExecutable();

            HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
            hiveCmdBuilder.addStatement(useDatabaseHql);
            hiveCmdBuilder.addStatement(dropTableHql);
            hiveCmdBuilder.addStatement(createTableHql);
            hiveCmdBuilder.addStatement(insertDataHqls);

            step.setCmd(hiveCmdBuilder.build());
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

            return step;
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            GarbageCollectionStep step = new GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
            step.setIntermediateTableIdentity(getIntermediateTableIdentity());
            step.setExternalDataPath(JoinedFlatTable.getTableDir(flatHiveTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobFlow.getId())));
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

            final String hiveTable = this.getIntermediateTableIdentity();
            if (config.isHiveKeepFlatTable() == false && StringUtils.isNotEmpty(hiveTable)) {
                final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
                hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
                hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS  " + hiveTable + ";");
                try {
                    config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
                    output.append("Hive table " + hiveTable + " is dropped. \n");

                    Path externalDataPath = new Path(getExternalDataPath());
                    FileSystem fs = FileSystem.get(externalDataPath.toUri(), HadoopUtil.getCurrentConfiguration());
                    if (fs.exists(externalDataPath)) {
                        fs.delete(externalDataPath, true);
                        output.append("Hive table " + hiveTable + " external data path " + externalDataPath + " is deleted. \n");
                    }
                } catch (IOException e) {
                    logger.error("job:" + getId() + " execute finished with exception", e);
                    return new ExecuteResult(ExecuteResult.State.ERROR, e.getMessage());
                }
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
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
    }

}
