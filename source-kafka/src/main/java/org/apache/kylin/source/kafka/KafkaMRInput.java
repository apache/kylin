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
package org.apache.kylin.source.kafka;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.hive.CreateFlatHiveTableStep;
import org.apache.kylin.source.hive.HiveMRInput;
import org.apache.kylin.source.kafka.hadoop.KafkaFlatTableJob;
import org.apache.kylin.source.kafka.job.MergeOffsetStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMRInput implements IMRInput {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMRInput.class);
    private CubeSegment cubeSegment;

    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        this.cubeSegment = (CubeSegment) flatDesc.getSegment();
        return new BatchCubingInputSide(cubeSegment, flatDesc);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table) {

        return new KafkaTableInputFormat(cubeSegment, null);
    }

    @Override
    public IMRBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new KafkaMRBatchMergeInputSide((CubeSegment) seg);
    }

    public static class KafkaTableInputFormat implements IMRTableInputFormat {
        private final CubeSegment cubeSegment;
        private final JobEngineConfig conf;
        private String delimiter = BatchConstants.SEQUENCE_FILE_DEFAULT_DELIMITER;

        public KafkaTableInputFormat(CubeSegment cubeSegment, JobEngineConfig conf) {
            this.cubeSegment = cubeSegment;
            this.conf = conf;
        }

        @Override
        public void configureJob(Job job) {
            job.setInputFormatClass(SequenceFileInputFormat.class);
            String jobId = job.getConfiguration().get(BatchConstants.ARG_CUBING_JOB_ID);
            IJoinedFlatTableDesc flatHiveTableDesc = new CubeJoinedFlatTableDesc(cubeSegment);
            String inputPath = JoinedFlatTable.getTableDir(flatHiveTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
            try {
                FileInputFormat.addInputPath(job, new Path(inputPath));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public Collection<String[]> parseMapperInput(Object mapperInput) {
            Text text = (Text) mapperInput;
            String[] columns  = Bytes.toString(text.getBytes(), 0, text.getLength()).split(delimiter);
            return Collections.singletonList(columns);
        }

    }

    public static class BatchCubingInputSide implements IMRBatchCubingInputSide {

        final JobEngineConfig conf;
        final CubeSegment seg;
        private CubeDesc cubeDesc ;
        private KylinConfig config;
        protected IJoinedFlatTableDesc flatDesc;
        protected String hiveTableDatabase;
        private List<String> intermediateTables = Lists.newArrayList();
        private List<String> intermediatePaths = Lists.newArrayList();
        private String cubeName;

        public BatchCubingInputSide(CubeSegment seg, IJoinedFlatTableDesc flatDesc) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.config = seg.getConfig();
            this.flatDesc = flatDesc;
            this.hiveTableDatabase = config.getHiveDatabaseForIntermediateTable();
            this.seg = seg;
            this.cubeDesc = seg.getCubeDesc();
            this.cubeName = seg.getCubeInstance().getName();
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {

            boolean onlyOneTable = cubeDesc.getModel().getLookupTables().size() == 0;
            final String baseLocation = getJobWorkingDir(jobFlow);
            if (onlyOneTable) {
                // directly use flat table location
                final String intermediateFactTable = flatDesc.getTableName();
                final String tableLocation = baseLocation + "/" + intermediateFactTable;
                jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId(), tableLocation));
                intermediatePaths.add(tableLocation);
            } else {
                final String mockFactTableName =  MetadataConstants.KYLIN_INTERMEDIATE_PREFIX + cubeName.toLowerCase() + "_"
                        + seg.getUuid().replaceAll("-", "_") + "_fact";
                jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId(), baseLocation + "/" + mockFactTableName));
                jobFlow.addTask(createFlatTable(mockFactTableName, baseLocation));
            }
        }
        private AbstractExecutable createFlatTable(final String mockFactTableName, String baseLocation) {
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(hiveTableDatabase);

            final IJoinedFlatTableDesc mockfactDesc = new IJoinedFlatTableDesc() {

                @Override
                public String getTableName() {
                    return mockFactTableName;
                }

                @Override
                public DataModelDesc getDataModel() {
                    return cubeDesc.getModel();
                }

                @Override
                public List<TblColRef> getAllColumns() {
                    return flatDesc.getFactColumns();
                }

                @Override
                public List<TblColRef> getFactColumns() {
                    return null;
                }

                @Override
                public int getColumnIndex(TblColRef colRef) {
                    return 0;
                }

                @Override
                public SegmentRange getSegRange() {
                    return null;
                }

                @Override
                public TblColRef getDistributedBy() {
                    return null;
                }

                @Override
                public TblColRef getClusterBy() {
                    return null;
                }

                @Override
                public ISegment getSegment() {
                    return null;
                }

                @Override
                public boolean useAlias() {
                    return false;
                }
            };
            final String dropFactTableHql = JoinedFlatTable.generateDropTableStatement(mockfactDesc);
            // the table inputformat is sequence file
            final String createFactTableHql = JoinedFlatTable.generateCreateTableStatement(mockfactDesc, baseLocation, JoinedFlatTable.SEQUENCEFILE);

            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, baseLocation);
            String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);
            insertDataHqls = insertDataHqls.replace(flatDesc.getDataModel().getRootFactTableName() + " ", mockFactTableName + " ");

            CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setInitStatement(hiveInitStatements);
            step.setCreateTableStatement(dropFactTableHql + createFactTableHql + dropTableHql + createTableHql + insertDataHqls);
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

            intermediateTables.add(flatDesc.getTableName());
            intermediateTables.add(mockFactTableName);
            intermediatePaths.add(baseLocation + "/" + flatDesc.getTableName());
            intermediatePaths.add(baseLocation + "/" + mockFactTableName);
            return step;
        }

        protected String getJobWorkingDir(DefaultChainedExecutable jobFlow) {
            return JobBuilderSupport.getJobWorkingDir(config.getHdfsWorkingDirectory(), jobFlow.getId());
        }

        private MapReduceExecutable createSaveKafkaDataStep(String jobId, String location) {
            MapReduceExecutable result = new MapReduceExecutable();
            result.setName("Save data from Kafka");
            result.setMapReduceJobClass(KafkaFlatTableJob.class);
            JobBuilderSupport jobBuilderSupport = new JobBuilderSupport(seg, "system");
            StringBuilder cmd = new StringBuilder();
            jobBuilderSupport.appendMapReduceParameters(cmd);
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, location);
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Save_Kafka_Data_" + seg.getRealization().getName() + "_Step");

            result.setMapReduceParams(cmd.toString());
            return result;
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            HiveMRInput.GarbageCollectionStep step = new HiveMRInput.GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_HIVE_CLEANUP);
            step.setIntermediateTables(intermediateTables);
            step.setExternalDataPaths(intermediatePaths);
            jobFlow.addTask(step);

        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new KafkaTableInputFormat(seg, conf);
        }
    }

    class KafkaMRBatchMergeInputSide implements IMRBatchMergeInputSide {

        private CubeSegment cubeSegment;

        KafkaMRBatchMergeInputSide(CubeSegment cubeSegment) {
            this.cubeSegment = cubeSegment;
        }

        @Override
        public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {

            final MergeOffsetStep result = new MergeOffsetStep();
            result.setName("Merge offset step");

            CubingExecutableUtil.setCubeName(cubeSegment.getCubeInstance().getName(), result.getParams());
            CubingExecutableUtil.setSegmentId(cubeSegment.getUuid(), result.getParams());
            CubingExecutableUtil.setCubingJobId(jobFlow.getId(), result.getParams());
            jobFlow.addTask(result);
        }
    }

    @Deprecated
    public static class GarbageCollectionStep extends AbstractExecutable {
        private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionStep.class);

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            try {
                rmdirOnHDFS(getDataPath());
            } catch (IOException e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                return ExecuteResult.createError(e);
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "HDFS path " + getDataPath() + " is dropped.\n");
        }

        private void rmdirOnHDFS(String path) throws IOException {
            Path externalDataPath = new Path(path);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(externalDataPath)) {
                fs.delete(externalDataPath, true);
            }
        }

        public void setDataPath(String externalDataPath) {
            setParam("dataPath", externalDataPath);
        }

        private String getDataPath() {
            return getParam("dataPath");
        }

    }
}
