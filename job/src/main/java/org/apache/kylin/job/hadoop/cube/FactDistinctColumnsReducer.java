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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();

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
                    hll.merge(cuboidHLLMap.get(cuboidId));
                }
                cuboidHLLMap.put(cuboidId, hll);
            }
        }

    }

    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {

        for (Long cuboidId : cuboidHLLMap.keySet()) {
            rowKeyCountInCuboids.put(cuboidId, cuboidHLLMap.get(cuboidId).getCountEstimate());
        }

        //output the hll info;
        if (collectStatistics) {
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
                
                for (long i = 0; i < baseCuboidId; i++) {
                    msg = "Cuboid " + i + " has " + rowKeyCountInCuboids.get(i) + " rows.";
                    out.write(msg.getBytes());
                    out.write('\n');
                }

            } finally {
                out.close();
            }
        }
    }

}
