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
package org.apache.kylin.storage.parquet.steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.parquet.example.data.Group;

/**
 */
public class ConvertToParquetReducer extends KylinReducer<Text, Text, NullWritable, Group> {
    private ParquetConvertor convertor;
    private MultipleOutputs<NullWritable, Group> mos;
    private CubeSegment cubeSegment;
    private CuboidToPartitionMapping mapping;

    @Override
    protected void doSetup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        super.bindCurrentConfiguration(conf);
        mos = new MultipleOutputs(context);

        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        String segmentId = conf.get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);
        cubeSegment = cube.getSegmentById(segmentId);
        SerializableConfiguration sConf = new SerializableConfiguration(conf);
        mapping = new CuboidToPartitionMapping(cubeSegment, kylinConfig);
        convertor = new ParquetConvertor(cubeName, segmentId, kylinConfig, sConf);
    }

    @Override
    protected void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long cuboidId = Bytes.toLong(key.getBytes(), RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
        int layerNumber = cubeSegment.getCuboidScheduler().getCuboidLayerMap().get(cuboidId);
        int partitionId = context.getTaskAttemptID().getTaskID().getId();

        for (Text value : values) {
            Group group = convertor.parseValueToGroup(key, value);
            String output = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel("", layerNumber) + "/"
                    + ParquetJobSteps.getCuboidOutputFileName(cuboidId, partitionId % mapping.getPartitionNumForCuboidId(cuboidId));
            mos.write(MRCubeParquetJob.BY_LAYER_OUTPUT, null, group, output);
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}