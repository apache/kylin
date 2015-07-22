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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.cmd.ShellCmdOutput;
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

public class HiveMRInput implements IMRInput {

    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        return new BatchCubingInputSide(seg);
    }
    
    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table) {
        return new HiveTableInputFormat(table.getIdentity());
    }
    
    public static class HiveTableInputFormat implements IMRTableInputFormat {
        final String dbName;
        final String tableName;

        public HiveTableInputFormat(String hiveTable) {
            String[] parts = HadoopUtil.parseHiveTableName(hiveTable);
            dbName = parts[0];
            tableName = parts[1];
        }

        @Override
        public void configureJob(Job job) {
            try {
                HCatInputFormat.setInput(job, dbName, tableName);
                job.setInputFormatClass(HCatInputFormat.class);
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
        final CubeSegment seg;
        final CubeJoinedFlatTableDesc flatHiveTableDesc;

        public BatchCubingInputSide(CubeSegment seg) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.seg = seg;
            this.flatHiveTableDesc = new CubeJoinedFlatTableDesc(seg.getCubeDesc(), seg);
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createFlatHiveTableStep(conf, flatHiveTableDesc, jobFlow.getId()));
        }
        
        public static AbstractExecutable createFlatHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId) {

            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
            String insertDataHqls;
            try {
                insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc, conf);
            } catch (IOException e) {
                throw new RuntimeException("Failed to generate insert data SQL for intermediate table.", e);
            }

            ShellExecutable step = new ShellExecutable();
            StringBuffer buf = new StringBuffer();
            buf.append("hive -e \"");
            buf.append(dropTableHql + "\n");
            buf.append(createTableHql + "\n");
            buf.append(insertDataHqls + "\n");
            buf.append("\"");

            step.setCmd(buf.toString());
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

            return step;
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            GarbageCollectionStep step = new GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
            step.setOldHiveTable(flatHiveTableDesc.getTableName());
            jobFlow.addTask(step);
        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new HiveTableInputFormat(flatHiveTableDesc.getTableName());
        }
        
    }
    
    public static class GarbageCollectionStep extends AbstractExecutable {

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            StringBuffer output = new StringBuffer();

            final String hiveTable = this.getOldHiveTable();
            if (StringUtils.isNotEmpty(hiveTable)) {
                final String dropHiveCMD = "hive -e \"DROP TABLE IF EXISTS  " + hiveTable + ";\"";
                ShellCmdOutput shellCmdOutput = new ShellCmdOutput();
                try {
                    context.getConfig().getCliCommandExecutor().execute(dropHiveCMD, shellCmdOutput);
                    output.append("Hive table " + hiveTable + " is dropped. \n");
                } catch (IOException e) {
                    logger.error("job:" + getId() + " execute finished with exception", e);
                    output.append(shellCmdOutput.getOutput()).append("\n").append(e.getLocalizedMessage());
                    return new ExecuteResult(ExecuteResult.State.ERROR, output.toString());
                }
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
        }

        public void setOldHiveTable(String hiveTable) {
            setParam("oldHiveTable", hiveTable);
        }

        private String getOldHiveTable() {
            return getParam("oldHiveTable");
        }
    }

}
