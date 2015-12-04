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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.steps.MergeCuboidMapper;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * This "Transition" impl generates cuboid files and then convert to HFile.
 * The additional step slows down build process, but the gains is merge
 * can read from HDFS instead of over HBase region server. See KYLIN-1007.
 * 
 * This is transitional because finally we want to merge from HTable snapshot.
 * However multiple snapshots as MR input is only supported by HBase 1.x.
 * Before most users upgrade to latest HBase, they can only use this transitional
 * cuboid file solution.
 */
public class HBaseMROutput2Transition implements IMROutput2 {

    private static final Logger logger = LoggerFactory.getLogger(HBaseMROutput2Transition.class);

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
        final CubeSegment seg;

        public HBaseInputFormat(CubeSegment seg) {
            this.seg = seg;
        }

        @Override
        public void configureInput(Class<? extends Mapper> mapperClz, Class<? extends WritableComparable> outputKeyClz, Class<? extends Writable> outputValueClz, Job job) throws IOException {
            // merge by cuboid files
            if (isMergeFromCuboidFiles(job.getConfiguration())) {
                logger.info("Merge from cuboid files for " + seg);
                
                job.setInputFormatClass(SequenceFileInputFormat.class);
                addCuboidInputDirs(job);
                
                job.setMapperClass(mapperClz);
                job.setMapOutputKeyClass(outputKeyClz);
                job.setMapOutputValueClass(outputValueClz);
            }
            // merge from HTable scan
            else {
                logger.info("Merge from HTables for " + seg);
                
                Configuration conf = job.getConfiguration();
                HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

                List<Scan> scans = new ArrayList<Scan>();
                for (String htable : new HBaseMRSteps(seg).getMergingHTables()) {
                    Scan scan = new Scan();
                    scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
                    scan.setCacheBlocks(false); // don't set to true for MR jobs
                    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(htable));
                    scans.add(scan);
                }
                TableMapReduceUtil.initTableMapperJob(scans, (Class<? extends TableMapper>) mapperClz, outputKeyClz, outputValueClz, job);
                TableMapReduceUtil.initCredentials(job);
            }
        }

        private void addCuboidInputDirs(Job job) throws IOException {
            List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
            HBaseMRSteps steps = new HBaseMRSteps(seg);
            
            String[] inputs = new String[mergingSegments.size()];
            for (int i = 0; i < mergingSegments.size(); i++) {
                CubeSegment mergingSeg = mergingSegments.get(i);
                String cuboidPath = steps.getCuboidRootPath(mergingSeg);
                inputs[i] = cuboidPath + (cuboidPath.endsWith("/") ? "" : "/") + "*";
            }
            
            AbstractHadoopJob.addInputDirs(inputs, job);
        }

        @Override
        public CubeSegment findSourceSegment(Context context) throws IOException {
            // merge by cuboid files
            if (isMergeFromCuboidFiles(context.getConfiguration())) {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                return MergeCuboidMapper.findSourceSegment(fileSplit, seg.getCubeInstance());
            }
            // merge from HTable scan
            else {
                TableSplit currentSplit = (TableSplit) context.getInputSplit();
                byte[] tableName = currentSplit.getTableName();
                String htableName = Bytes.toString(tableName);

                // decide which source segment
                for (CubeSegment segment : seg.getCubeInstance().getSegments()) {
                    String segmentHtable = segment.getStorageLocationIdentifier();
                    if (segmentHtable != null && segmentHtable.equalsIgnoreCase(htableName)) {
                        return segment;
                    }
                }
                throw new IllegalStateException("No merging segment's storage location identifier equals " + htableName);
            }
        }

        transient ByteArrayWritable parsedKey;
        transient Object[] parsedValue;
        transient Pair<ByteArrayWritable, Object[]> parsedPair;
        
        transient MeasureCodec measureCodec;
        transient RowValueDecoder[] rowValueDecoders;

        @Override
        public Pair<ByteArrayWritable, Object[]> parseMapperInput(Object inKey, Object inValue) {
            // lazy init
            if (parsedPair == null) {
                parsedKey = new ByteArrayWritable();
                parsedValue = new Object[seg.getCubeDesc().getMeasures().size()];
                parsedPair = Pair.newPair(parsedKey, parsedValue);
            }
            
            // merge by cuboid files
            if (isMergeFromCuboidFiles(null)) {
                return parseMapperInputFromCuboidFile(inKey, inValue);
            }
            // merge from HTable scan
            else {
                return parseMapperInputFromHTable(inKey, inValue);
            }
        }

        private Pair<ByteArrayWritable, Object[]> parseMapperInputFromCuboidFile(Object inKey, Object inValue) {
            // lazy init
            if (measureCodec == null) {
                measureCodec = new MeasureCodec(seg.getCubeDesc().getMeasures());
            }
            
            Text key = (Text) inKey;
            parsedKey.set(key.getBytes(), 0, key.getLength());
            
            Text value = (Text) inValue;
            measureCodec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), parsedValue);
            
            return parsedPair;
        }

        private Pair<ByteArrayWritable, Object[]> parseMapperInputFromHTable(Object inKey, Object inValue) {
            // lazy init
            if (rowValueDecoders == null) {
                List<RowValueDecoder> valueDecoderList = Lists.newArrayList();
                List<MeasureDesc> measuresDescs = Lists.newArrayList();
                for (HBaseColumnFamilyDesc cfDesc : seg.getCubeDesc().getHbaseMapping().getColumnFamily()) {
                    for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                        valueDecoderList.add(new RowValueDecoder(colDesc));
                        for (MeasureDesc measure : colDesc.getMeasures()) {
                            measuresDescs.add(measure);
                        }
                    }
                }
                rowValueDecoders = valueDecoderList.toArray(new RowValueDecoder[valueDecoderList.size()]);
            }

            ImmutableBytesWritable key = (ImmutableBytesWritable) inKey;
            parsedKey.set(key.get(), key.getOffset(), key.getLength());

            Result hbaseRow = (Result) inValue;
            for (int i = 0; i < rowValueDecoders.length; i++) {
                rowValueDecoders[i].decode(hbaseRow);
                rowValueDecoders[i].loadCubeMeasureArray(parsedValue);
            }

            return parsedPair;
        }

        transient Boolean isMergeFromCuboidFiles;

        // merge from cuboid files is better than merge from HTable, because no workload on HBase region server
        private boolean isMergeFromCuboidFiles(Configuration jobConf) {
            // cache in this object?
            if (isMergeFromCuboidFiles != null)
                return isMergeFromCuboidFiles.booleanValue();

            final String confKey = "kylin.isMergeFromCuboidFiles";

            // cache in job configuration?
            if (jobConf != null) {
                String result = jobConf.get(confKey);
                if (result != null) {
                    isMergeFromCuboidFiles = Boolean.valueOf(result);
                    return isMergeFromCuboidFiles.booleanValue();
                }
            }

            boolean result = true;

            try {
                List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
                HBaseMRSteps steps = new HBaseMRSteps(seg);
                for (CubeSegment mergingSeg : mergingSegments) {
                    String cuboidRootPath = steps.getCuboidRootPath(mergingSeg);
                    FileSystem fs = HadoopUtil.getFileSystem(cuboidRootPath);

                    boolean cuboidFileExist = fs.exists(new Path(cuboidRootPath));
                    if (cuboidFileExist == false) {
                        logger.info("Merge from HTable because " + cuboidRootPath + " does not exist");
                        result = false;
                        break;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // put in cache
            isMergeFromCuboidFiles = Boolean.valueOf(result);
            if (jobConf != null) {
                jobConf.set(confKey, "" + result);
            }
            return result;
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

            // estimate the reducer number
            String htableName = seg.getStorageLocationIdentifier();
            Configuration conf = HBaseConfiguration.create(job.getConfiguration());
            HTable htable = new HTable(conf, htableName);
            int regions = htable.getStartKeys().length + 1;
            htable.close();
            
            int reducerNum = regions * 3;
            reducerNum = Math.max(1, reducerNum);
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            reducerNum = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), reducerNum);

            job.setNumReduceTasks(reducerNum);

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
