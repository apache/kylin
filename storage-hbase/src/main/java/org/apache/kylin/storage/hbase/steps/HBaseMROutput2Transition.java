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

package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;

import com.google.common.collect.Lists;

/**
 * This "Transition" MR output generates cuboid files and then convert to HFile.
 * The additional step slows down build process slightly, but the gains is merge
 * can read from HDFS instead of over HBase region server. See KYLIN-1007.
 * 
 * This is transitional because finally we want to merge from HTable snapshot.
 * However MR input with multiple snapshots is only supported by HBase 1.x.
 * Before most users upgrade to latest HBase, they can only use this transitional
 * cuboid file solution.
 */
public class HBaseMROutput2Transition implements IMROutput2 {

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
                String cuboidRootPath = steps.getCuboidRootPath(jobFlow.getId());
                
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(cuboidRootPath, jobFlow.getId()));
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
                String cuboidRootPath = steps.getCuboidRootPath(jobFlow.getId());
                
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(cuboidRootPath, jobFlow.getId()));
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

            Result hbaseRow = (Result) inValue;
            for (int i = 0; i < rowValueDecoders.length; i++) {
                rowValueDecoders[i].decode(hbaseRow);
                rowValueDecoders[i].loadCubeMeasureArray(parsedValue);
            }

            return parsedPair;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static class HBaseOutputFormat implements IMRStorageOutputFormat {
        final CubeSegment seg;

        Text outputKey;
        Text outputValue;
        ByteBuffer valueBuf;
        MeasureCodec codec;

        public HBaseOutputFormat(CubeSegment seg) {
            this.seg = seg;
        }

        @Override
        public void configureOutput(Class<? extends Reducer> reducer, String jobFlowId, Job job) throws IOException {
            job.setReducerClass(reducer);

            // the cuboid file and KV class must be compatible with 0.7 version for smooth upgrade
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            Path output = new Path(new HBaseMRSteps(seg).getCuboidRootPath(jobFlowId));
            FileOutputFormat.setOutputPath(job, output);
            
            HadoopUtil.deletePath(job.getConfiguration(), output);
        }

        @Override
        public void doReducerOutput(ByteArrayWritable key, Object[] value, Reducer.Context context) throws IOException, InterruptedException {
            // lazy init
            if (outputKey == null) {
                outputKey = new Text();
                outputValue = new Text();
                valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
                codec = new MeasureCodec(seg.getCubeDesc().getMeasures());
            }

            outputKey.set(key.array(), key.offset(), key.length());
         
            valueBuf.clear();
            codec.encode(value, valueBuf);
            outputValue.set(valueBuf.array(), 0, valueBuf.position());
            
            context.write(outputKey, outputValue);
        }
    }

}
