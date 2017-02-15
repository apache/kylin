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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.cube.CubeSegment;
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
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.hadoop.KafkaFlatTableJob;
import org.apache.kylin.source.kafka.job.MergeOffsetStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class KafkaMRInput implements IMRInput {

    CubeSegment cubeSegment;

    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        this.cubeSegment = (CubeSegment) flatDesc.getSegment();
        return new BatchCubingInputSide(cubeSegment);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table) {
       return getTableInputFormat(table, true);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table, boolean isFullTable) {
        KafkaConfigManager kafkaConfigManager = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv());
        KafkaConfig kafkaConfig = kafkaConfigManager.getKafkaConfig(table.getIdentity());
        List<TblColRef> columns = Lists.transform(Arrays.asList(table.getColumns()), new Function<ColumnDesc, TblColRef>() {
            @Nullable
            @Override
            public TblColRef apply(ColumnDesc input) {
                return input.getRef();
            }
        });

        return new KafkaTableInputFormat(cubeSegment, columns, kafkaConfig, null);
    }

    @Override
    public IMRBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new KafkaMRBatchMergeInputSide((CubeSegment) seg);
    }

    public static class KafkaTableInputFormat implements IMRTableInputFormat {
        private final CubeSegment cubeSegment;
        private StreamingParser streamingParser;
        private final JobEngineConfig conf;

        public KafkaTableInputFormat(CubeSegment cubeSegment, List<TblColRef> columns, KafkaConfig kafkaConfig, JobEngineConfig conf) {
            this.cubeSegment = cubeSegment;
            this.conf = conf;
            try {
                streamingParser = StreamingParser.getStreamingParser(kafkaConfig.getParserName(), kafkaConfig.getParserProperties(), columns);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public void configureJob(Job job) {
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputValueClass(Text.class);
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
        public String[] parseMapperInput(Object mapperInput) {
            Text text = (Text) mapperInput;
            ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());
            StreamingMessage streamingMessage = streamingParser.parse(buffer);
            return streamingMessage.getData().toArray(new String[streamingMessage.getData().size()]);
        }

    }

    public static class BatchCubingInputSide implements IMRBatchCubingInputSide {

        final JobEngineConfig conf;
        final CubeSegment seg;
        private String outputPath;

        public BatchCubingInputSide(CubeSegment seg) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.seg = seg;
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId()));
        }

        private MapReduceExecutable createSaveKafkaDataStep(String jobId) {
            MapReduceExecutable result = new MapReduceExecutable();

            IJoinedFlatTableDesc flatHiveTableDesc = new CubeJoinedFlatTableDesc(seg);
            outputPath = JoinedFlatTable.getTableDir(flatHiveTableDesc, JobBuilderSupport.getJobWorkingDir(conf, jobId));
            result.setName("Save data from Kafka");
            result.setMapReduceJobClass(KafkaFlatTableJob.class);
            JobBuilderSupport jobBuilderSupport = new JobBuilderSupport(seg, "system");
            StringBuilder cmd = new StringBuilder();
            jobBuilderSupport.appendMapReduceParameters(cmd);
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
            JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Save_Kafka_Data_" + seg.getRealization().getName() + "_Step");

            result.setMapReduceParams(cmd.toString());
            return result;
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            GarbageCollectionStep step = new GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_KAFKA_CLEANUP);
            step.setDataPath(outputPath);
            jobFlow.addTask(step);

        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            KafkaConfigManager kafkaConfigManager = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv());
            KafkaConfig kafkaConfig = kafkaConfigManager.getKafkaConfig(seg.getCubeInstance().getRootFactTable());
            List<TblColRef> columns = new CubeJoinedFlatTableDesc(seg).getAllColumns();

            return new KafkaTableInputFormat(seg, columns, kafkaConfig, conf);
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

            CubingExecutableUtil.setCubeName(cubeSegment.getRealization().getName(), result.getParams());
            CubingExecutableUtil.setSegmentId(cubeSegment.getUuid(), result.getParams());
            CubingExecutableUtil.setCubingJobId(jobFlow.getId(), result.getParams());
            jobFlow.addTask(result);
        }
    }

    public static class GarbageCollectionStep extends AbstractExecutable {
        private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionStep.class);

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            try {
                rmdirOnHDFS(getDataPath());
            } catch (IOException e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                return new ExecuteResult(ExecuteResult.State.ERROR, e.getMessage());
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
