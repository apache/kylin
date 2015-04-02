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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author yangli9
 */
public class FactDistinctColumnsReducer extends KylinReducer<LongWritable, Text, NullWritable, Text> {

    private List<TblColRef> columnList = new ArrayList<TblColRef>();
    private boolean collectStatistics = false;
    private String statisticsOutput = null;
    private List<Long> rowKeyCountInMappers;
    private Map<Long, Long> rowKeyCountInCuboids;
    protected Map<Long, HyperLogLogPlusCounter> cuboidHLLMap = null;
    protected long baseCuboidId;
    protected CubeDesc cubeDesc;

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
            rowKeyCountInMappers = Lists.newArrayList();
            rowKeyCountInCuboids = Maps.newHashMap();
            cuboidHLLMap = Maps.newHashMap();
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

                if (cuboidId > baseCuboidId) {
                    // if this is the summary info from a mapper, record the number before merge
                    rowKeyCountInMappers.add(hll.getCountEstimate());
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
                rowKeyCountInCuboids.put(cuboidId, cuboidHLLMap.get(cuboidId).getCountEstimate());
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
            long totalSum = 0;
            String msg;
            for (int i = 0; i < rowKeyCountInMappers.size(); i++) {
                msg = "Cube segment in Mapper " + i + " has " + rowKeyCountInMappers.get(i) + " rows.";
                totalSum += rowKeyCountInMappers.get(i);
                out.write(msg.getBytes());
                out.write('\n');
            }

            msg = "Sum of the cube segments is " + totalSum;
            out.write(msg.getBytes());
            out.write('\n');


            long grantTotal = rowKeyCountInCuboids.get(baseCuboidId + 1);
            msg = "The merged cube has " + grantTotal + " rows.";
            out.write(msg.getBytes());
            out.write('\n');

            msg = "The compaction rate is " + (grantTotal) + "/" + totalSum + " = " + (grantTotal * 100.0) / totalSum + "%.";
            out.write(msg.getBytes());
            out.write('\n');
            out.write('\n');

            List<Long> allCuboids = new ArrayList<Long>();
            allCuboids.addAll(rowKeyCountInCuboids.keySet());
            Collections.sort(allCuboids);
            for (long i : allCuboids) {
                if (i <= baseCuboidId) {
                    msg = "Cuboid " + i + " has " + rowKeyCountInCuboids.get(i) + " rows.";
                } else {
                    msg = "Totally the cube has " + rowKeyCountInCuboids.get(i) + " rows.";
                }
                out.write(msg.getBytes());
                out.write('\n');
            }

        } finally {
            out.close();
        }
    }


    private void writeCuboidStatistics(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path seqFilePath = new Path(statisticsOutput, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION);
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(seqFilePath), SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(LongWritable.class));

        List<Long> allCuboids = new ArrayList<Long>();
        allCuboids.addAll(rowKeyCountInCuboids.keySet());
        Collections.sort(allCuboids);
        try {
            for (long i : allCuboids) {
                if (i <= baseCuboidId) {
                    writer.append(new LongWritable(i), new LongWritable(rowKeyCountInCuboids.get(i)));
                }
            }
        } finally {
            writer.close();
        }

    }
}
