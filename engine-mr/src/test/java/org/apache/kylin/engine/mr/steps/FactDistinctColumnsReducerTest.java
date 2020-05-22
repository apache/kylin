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

package org.apache.kylin.engine.mr.steps;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;

public class FactDistinctColumnsReducerTest extends LocalFileMetadataTestCase {
    private String cubeName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private ReduceDriver<SelfDefineSortableKey, Text, NullWritable, Text> reduceDriver;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        FileUtils.deleteDirectory(new File("./meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl().toString()), new File("./meta"));

        cubeName = "test_kylin_cube_with_slr_1_new_segment";
        cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        FactDistinctColumnsReducer factDistinctColumnsReducer = new FactDistinctColumnsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(factDistinctColumnsReducer);
    }

    @After
    public void after() throws Exception {
        FileUtils.deleteDirectory(new File("./meta"));
        FileUtils.deleteQuietly(new File(FileOutputCommitter.PENDING_DIR_NAME));
        cleanupTestMetadata();
    }

    @Test
    public void testWriteCuboidStatistics() throws IOException {

        final Configuration conf = HadoopUtil.getCurrentConfiguration();
        File tmp = File.createTempFile("cuboidstatistics", "");
        final Path outputPath = new Path(tmp.getParent() + File.separator + RandomUtil.randomUUID().toString());
        if (!FileSystem.getLocal(conf).exists(outputPath)) {
            //            FileSystem.getLocal(conf).create(outputPath);
        }

        System.out.println(outputPath);
        Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();
        CubeStatsWriter.writeCuboidStatistics(conf, outputPath, cuboidHLLMap, 100);
        FileSystem.getLocal(conf).delete(outputPath, true);

    }

    @Test
    public void testReducerStatistics() throws IOException {
        setConfigurations();
        setMultipleOutputs(BatchConstants.CFG_OUTPUT_STATISTICS, reduceDriver.getConfiguration(),
                SequenceFileOutputFormat.class, LongWritable.class, BytesWritable.class);
        setMultipleOutputs(BatchConstants.CFG_OUTPUT_PARTITION, reduceDriver.getConfiguration(), TextOutputFormat.class,
                NullWritable.class, LongWritable.class);

        // override the task id
        int dimColsSize = cubeDesc.getRowkey().getRowKeyColumns().length;
        int uhcSize = cubeDesc.getAllUHCColumns().size();
        final int targetTaskId = (dimColsSize - uhcSize) + uhcSize * cubeDesc.getConfig().getUHCReducerCount();

        setContextTaskId(targetTaskId);
        ByteBuffer tmpBuf = ByteBuffer.allocate(4096);
        tmpBuf.put((byte) FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER); // one byte
        tmpBuf.putLong(100);
        Text outputKey1 = new Text();
        outputKey1.set(tmpBuf.array(), 0, tmpBuf.position());
        SelfDefineSortableKey key1 = new SelfDefineSortableKey();
        key1.init(outputKey1, (byte) 0);

        HLLCounter hll = createMockHLLCounter();
        ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
        hllBuf.clear();
        hll.writeRegisters(hllBuf);
        Text value1 = new Text();
        value1.set(hllBuf.array(), 0, hllBuf.position());

        reduceDriver.setInput(key1, ImmutableList.of(value1));

        List<Pair<NullWritable, Text>> result = reduceDriver.run();
        assertEquals(0, result.size()); // the reducer output statistics info to a sequence file.
    }

    @Test
    public void testReducerNormalDimDictInReducer() throws IOException {
        testNormalDim();
    }

    @Test
    public void testReducerNormalDim() throws IOException {
        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
        kylinConfig.setProperty("kylin.engine.mr.build-dict-in-reducer", "false");
        testNormalDim();
    }

    private void setContextTaskId(final int taskId) {
        Context context = reduceDriver.getContext();
        when(context.getTaskAttemptID()).thenAnswer(new Answer<TaskAttemptID>() {
            @Override
            public TaskAttemptID answer(InvocationOnMock invocation) throws Throwable {
                return TaskAttemptID.forName("attempt__0000_r_" + taskId + "_0");
            }
        });
    }

    private void setConfigurations() {
        Configuration configuration = reduceDriver.getConfiguration();
        configuration.set(BatchConstants.CFG_CUBE_NAME, "test_kylin_cube_with_slr_1_new_segment");
        configuration.set(BatchConstants.CFG_CUBE_SEGMENT_ID, "198va32a-a33e-4b69-83dd-0bb8b1f8c53b");
        configuration.set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, "100");
        configuration.set(FileOutputFormat.OUTDIR, ".");
    }

    // copy from MultpleOutputs for test
    private static final String MULTIPLE_OUTPUTS = "mapreduce.multipleoutputs";
    private static final String MO_PREFIX = "mapreduce.multipleoutputs.namedOutput.";
    private static final String FORMAT = ".format";
    private static final String KEY = ".key";
    private static final String VALUE = ".value";

    private void setMultipleOutputs(String namedOutput, Configuration conf,
            Class<? extends OutputFormat> outputFormatClass, Class<?> keyClass, Class<?> valueClass)
            throws IOException {
        conf.set(MULTIPLE_OUTPUTS, conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
        conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass, OutputFormat.class);
        conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
        conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
    }

    private HLLCounter createMockHLLCounter() {
        HLLCounter hllc = new HLLCounter(14);
        HLLCounter one = new HLLCounter(14);
        for (int i = 0; i < 1000; i++) {
            one.clear();
            one.add(i);
            hllc.merge(one);
        }
        return hllc;
    }

    private void testNormalDim() throws IOException {
        setConfigurations();
        setMultipleOutputs(BatchConstants.CFG_OUTPUT_COLUMN, reduceDriver.getConfiguration(),
                SequenceFileOutputFormat.class, NullWritable.class, Text.class);
        setMultipleOutputs(BatchConstants.CFG_OUTPUT_DICT, reduceDriver.getConfiguration(),
                SequenceFileOutputFormat.class, NullWritable.class, ArrayPrimitiveWritable.class);
        setMultipleOutputs(BatchConstants.CFG_OUTPUT_PARTITION, reduceDriver.getConfiguration(), TextOutputFormat.class,
                NullWritable.class, LongWritable.class);

        int nDimReducers = cubeDesc.getRowkey().getRowKeyColumns().length;
        setContextTaskId(nDimReducers - 1);

        ByteBuffer tmpBuf = ByteBuffer.allocate(4096);
        String val = "100";
        tmpBuf.put(Bytes.toBytes(val));
        Text outputKey1 = new Text();
        outputKey1.set(tmpBuf.array(), 0, tmpBuf.position());
        SelfDefineSortableKey key1 = new SelfDefineSortableKey();
        key1.init(outputKey1, (byte) 0);

        reduceDriver.setInput(key1, ImmutableList.of(new Text()));
        List<Pair<NullWritable, Text>> result = reduceDriver.run();
        assertEquals(0, result.size());
    }

}
