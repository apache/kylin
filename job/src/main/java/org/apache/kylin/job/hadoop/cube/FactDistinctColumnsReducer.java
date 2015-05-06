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

package org.apache.kylin.job.hadoop.cube;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;
import java.util.*;

/**
 * @author yangli9
 */
public class FactDistinctColumnsReducer extends KylinReducer<LongWritable, Text, NullWritable, Text> {

    private List<TblColRef> columnList = new ArrayList<TblColRef>();
    private boolean collectStatistics = false;
    private String statisticsOutput = null;
    private List<Long> baseCuboidRowCountInMappers;
    private Map<Long, Long> rowCountInCuboids;
    protected Map<Long, HyperLogLogPlusCounter> cuboidHLLMap = null;
    protected long baseCuboidId;
    protected CubeDesc cubeDesc;
    private long totalRowsBeforeMerge = 0;
    private int SAMPING_PERCENTAGE = 5;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();

        baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        columnList = baseCuboid.getColumns();
        collectStatistics = Boolean.parseBoolean(conf.get(BatchConstants.CFG_STATISTICS_ENABLED));
        statisticsOutput = conf.get(BatchConstants.CFG_STATISTICS_OUTPUT);

        if (collectStatistics) {
            baseCuboidRowCountInMappers = Lists.newArrayList();
            rowCountInCuboids = Maps.newHashMap();
            cuboidHLLMap = Maps.newHashMap();
            SAMPING_PERCENTAGE = Integer.parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, "5"));
        }
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (key.get() >= 0) {
            TblColRef col = columnList.get((int) key.get());

            HashSet<ByteArray> set = new HashSet<ByteArray>();
            for (Text textValue : values) {
                ByteArray value = new ByteArray(Bytes.copy(textValue.getBytes(), 0, textValue.getLength()));
                set.add(value);
            }

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            String outputPath = conf.get(BatchConstants.OUTPUT_PATH);
            FSDataOutputStream out = fs.create(new Path(outputPath, col.getName()));

            try {
                for (ByteArray value : set) {
                    out.write(value.array(), value.offset(), value.length());
                    out.write('\n');
                }
            } finally {
                out.close();
            }
        } else {
            // for hll
            long cuboidId = 0 - key.get();

                for (Text value : values) {
                    HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(16);
                    ByteArray byteArray = new ByteArray(value.getBytes());
                    hll.readRegisters(byteArray.asBuffer());

                    totalRowsBeforeMerge += hll.getCountEstimate();

                    if (cuboidId == baseCuboidId) {
                        baseCuboidRowCountInMappers.add(hll.getCountEstimate());
                    }

                    if (cuboidHLLMap.get(cuboidId) != null) {
                        cuboidHLLMap.get(cuboidId).merge(hll);
                    } else {
                        cuboidHLLMap.put(cuboidId, hll);
                    }
                }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //output the hll info;
        if (collectStatistics) {
            for (Long cuboidId : cuboidHLLMap.keySet()) {
                rowCountInCuboids.put(cuboidId, cuboidHLLMap.get(cuboidId).getCountEstimate());
            }

            writeMapperAndCuboidStatistics(context); // for human check
            writeCuboidStatistics(context); // for CreateHTableJob
        }
    }

    private void writeMapperAndCuboidStatistics(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(statisticsOutput, BatchConstants.CFG_STATISTICS_CUBE_ESTIMATION));

        try {
            String msg;

            List<Long> allCuboids = new ArrayList<Long>();
            allCuboids.addAll(rowCountInCuboids.keySet());
            Collections.sort(allCuboids);

            msg = "Total cuboid number: \t" + allCuboids.size();
            writeLine(out, msg);
            msg = "Samping percentage: \t" + SAMPING_PERCENTAGE;
            writeLine(out, msg);

            writeLine(out, "The following statistics are collected based sampling data.");
            for (int i = 0; i < baseCuboidRowCountInMappers.size(); i++) {
                if (baseCuboidRowCountInMappers.get(i) > 0) {
                    msg = "Base Cuboid in Mapper " + i + " row count: \t " + baseCuboidRowCountInMappers.get(i);
                    writeLine(out, msg);
                }
            }

            long grantTotal = 0;
            for (long i : allCuboids) {
                grantTotal += rowCountInCuboids.get(i);
                msg = "Cuboid " + i + " row count is: \t " + rowCountInCuboids.get(i);
                writeLine(out, msg);
            }

            msg = "Sum of all the cube segments (before merge) is: \t " + totalRowsBeforeMerge;
            writeLine(out, msg);

            msg = "After merge, the cube has row count: \t " + grantTotal;
            writeLine(out, msg);

            msg = "The compaction factor is: \t" + totalRowsBeforeMerge / grantTotal;
            writeLine(out, msg);

        } finally {
            out.close();
        }
    }

    private void writeLine(FSDataOutputStream out, String msg) throws IOException {
        out.write(msg.getBytes());
        out.write('\n');

    }

    private void writeCuboidStatistics(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path seqFilePath = new Path(statisticsOutput, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION);
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(seqFilePath), SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(LongWritable.class));

        List<Long> allCuboids = new ArrayList<Long>();
        allCuboids.addAll(rowCountInCuboids.keySet());
        Collections.sort(allCuboids);
        try {
            for (long i : allCuboids) {
                writer.append(new LongWritable(i), new LongWritable((long) (rowCountInCuboids.get(i) *100 / SAMPING_PERCENTAGE)));
            }
        } finally {
            writer.close();
        }

    }
}
