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

package org.apache.kylin.storage.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.storage.hbase.steps.HBaseMRSteps;
import org.apache.kylin.storage.hbase.steps.InMemKeyValueCreator;
import org.apache.kylin.storage.hbase.steps.RowValueDecoder;

import com.google.common.collect.Lists;

public class HBaseMROutput2 implements IMROutput2 {

    @Override
    public IMRBatchCubingOutputSide2 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMRBatchCubingOutputSide2() {
            HBaseMRSteps steps = new HBaseMRSteps(seg);

            @Override
            public IMRStorageOutputFormat getStorageOutputFormat() {
                return new HBaseOutputFormat(seg);
            }

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStepWithStats(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                // nothing to do
            }
        };
    }

    @Override
    public IMRBatchMergeInputSide2 getBatchMergeInputSide(final CubeSegment seg) {
        return new IMRBatchMergeInputSide2() {
            @Override
            public IMRStorageInputFormat getStorageInputFormat() {
                return new HBaseInputFormat(seg);
            }
        };
    }

    @Override
    public IMRBatchMergeOutputSide2 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMRBatchMergeOutputSide2() {
            HBaseMRSteps steps = new HBaseMRSteps(seg);

            @Override
            public IMRStorageOutputFormat getStorageOutputFormat() {
                return new HBaseOutputFormat(seg);
            }

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStepWithStats(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createMergeGCStep());
            }
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static class HBaseInputFormat implements IMRStorageInputFormat {
        final Iterable<String> htables;

        final RowValueDecoder[] rowValueDecoders;
        final ByteArrayWritable parsedKey;
        final Object[] parsedValue;
        final Pair<ByteArrayWritable, Object[]> parsedPair;

        public HBaseInputFormat(CubeSegment seg) {
            this.htables = new HBaseMRSteps(seg).getMergingHTables();

            List<RowValueDecoder> valueDecoderList = Lists.newArrayList();
            List<MeasureDesc> measuresDescs = Lists.newArrayList();
            for (HBaseColumnFamilyDesc cfDesc : seg.getCubeDesc().getHBaseMapping().getColumnFamily()) {
                for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                    valueDecoderList.add(new RowValueDecoder(colDesc));
                    for (MeasureDesc measure : colDesc.getMeasures()) {
                        measuresDescs.add(measure);
                    }
                }
            }
            this.rowValueDecoders = valueDecoderList.toArray(new RowValueDecoder[valueDecoderList.size()]);

            this.parsedKey = new ByteArrayWritable();
            this.parsedValue = new Object[measuresDescs.size()];
            this.parsedPair = new Pair<ByteArrayWritable, Object[]>(parsedKey, parsedValue);
        }

        @Override
        public void configureInput(Class<? extends Mapper> mapper, Class<? extends WritableComparable> outputKeyClass, Class<? extends Writable> outputValueClass, Job job) throws IOException {
            Configuration conf = job.getConfiguration();
            HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
            
            List<Scan> scans = new ArrayList<Scan>();
            for (String htable : htables) {
                Scan scan = new Scan();
                scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
                scan.setCacheBlocks(false); // don't set to true for MR jobs
                scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(htable));
                scans.add(scan);
            }
            TableMapReduceUtil.initTableMapperJob(scans, (Class<? extends TableMapper>) mapper, outputKeyClass, outputValueClass, job);
        }

        @Override
        public CubeSegment findSourceSegment(Context context, CubeInstance cubeInstance) throws IOException {
            TableSplit currentSplit = (TableSplit) context.getInputSplit();
            byte[] tableName = currentSplit.getTableName();
            String htableName = Bytes.toString(tableName);

            // decide which source segment
            for (CubeSegment segment : cubeInstance.getSegments()) {
                String segmentHtable = segment.getStorageLocationIdentifier();
                if (segmentHtable != null && segmentHtable.equalsIgnoreCase(htableName)) {
                    return segment;
                }
            }
            throw new IllegalStateException("No merging segment's storage location identifier equals " + htableName);
        }

        @Override
        public Pair<ByteArrayWritable, Object[]> parseMapperInput(Object inKey, Object inValue) {
            ImmutableBytesWritable key = (ImmutableBytesWritable) inKey;
            parsedKey.set(key.get(), key.getOffset(), key.getLength());

            Result value = (Result) inValue;
            int position = 0;
            for (int i = 0; i < rowValueDecoders.length; i++) {
                rowValueDecoders[i].decode(value, false);
                Object[] measureValues = rowValueDecoders[i].getValues();
                for (int j = 0; j < measureValues.length; j++) {
                    parsedValue[position++] = measureValues[j];
                }
            }

            return parsedPair;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static class HBaseOutputFormat implements IMRStorageOutputFormat {
        final CubeSegment seg;

        final List<InMemKeyValueCreator> keyValueCreators = Lists.newArrayList();
        final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();

        public HBaseOutputFormat(CubeSegment seg) {
            this.seg = seg;
        }

        @Override
        public void configureOutput(Class<? extends Reducer> reducer, String jobFlowId, Job job) throws IOException {
            Path hfilePath = new Path(new HBaseMRSteps(seg).getHFilePath(jobFlowId));
            FileOutputFormat.setOutputPath(job, hfilePath);

            String htableName = seg.getStorageLocationIdentifier();
            Configuration conf = HBaseConfiguration.create(job.getConfiguration());
            HTable htable = new HTable(conf, htableName);
            HFileOutputFormat.configureIncrementalLoad(job, htable);

            // set Reducer; This need be after configureIncrementalLoad, to overwrite the default reducer class
            job.setReducerClass(reducer);

            // kylin uses ByteArrayWritable instead of ImmutableBytesWritable as mapper output key
            rewriteTotalOrderPartitionerFile(job);

            HadoopUtil.deletePath(job.getConfiguration(), hfilePath);
        }

        @Override
        public void doReducerOutput(ByteArrayWritable key, Object[] value, Reducer.Context context) throws IOException, InterruptedException {
            if (keyValueCreators.size() == 0) {
                int startPosition = 0;
                for (HBaseColumnFamilyDesc cfDesc : seg.getCubeDesc().getHBaseMapping().getColumnFamily()) {
                    for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                        keyValueCreators.add(new InMemKeyValueCreator(colDesc, startPosition));
                        startPosition += colDesc.getMeasures().length;
                    }
                }
            }

            outputKey.set(key.array(), key.offset(), key.length());

            KeyValue outputValue;
            for (int i = 0; i < keyValueCreators.size(); i++) {
                outputValue = keyValueCreators.get(i).create(key.array(), key.offset(), key.length(), value);
                context.write(outputKey, outputValue);
            }
        }

        private void rewriteTotalOrderPartitionerFile(Job job) throws IOException {
            Configuration conf = job.getConfiguration();
            String partitionsFile = TotalOrderPartitioner.getPartitionFile(conf);
            if (StringUtils.isBlank(partitionsFile))
                throw new IllegalStateException("HFileOutputFormat.configureIncrementalLoad don't configure TotalOrderPartitioner any more?");

            Path partitionsPath = new Path(partitionsFile);

            // read in partition file in ImmutableBytesWritable
            List<ByteArrayWritable> keys = Lists.newArrayList();
            Reader reader = new SequenceFile.Reader(conf, Reader.file(partitionsPath));
            try {
                ImmutableBytesWritable key = new ImmutableBytesWritable();
                while (reader.next(key, NullWritable.get())) {
                    keys.add(new ByteArrayWritable(key.copyBytes()));
                }
            } finally {
                reader.close();
            }

            // write out again in ByteArrayWritable
            Writer writer = SequenceFile.createWriter(conf, Writer.file(partitionsPath), Writer.keyClass(ByteArrayWritable.class), Writer.valueClass(NullWritable.class));
            try {
                for (ByteArrayWritable key : keys) {
                    writer.append(key, NullWritable.get());
                }
            } finally {
                writer.close();
            }
        }

    }

}
