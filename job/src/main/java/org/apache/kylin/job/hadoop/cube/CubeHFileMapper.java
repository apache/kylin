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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.measure.MeasureCodec;

import com.google.common.collect.Lists;

/**
 * @author George Song (ysong1)
 * 
 */
public class CubeHFileMapper extends KylinMapper<Text, Text, ImmutableBytesWritable, KeyValue> {

    ImmutableBytesWritable outputKey = new ImmutableBytesWritable();

    String cubeName;
    CubeDesc cubeDesc;

    MeasureCodec inputCodec;
    Object[] inputMeasures;
    List<KeyValueCreator> keyValueCreators;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeManager cubeMgr = CubeManager.getInstance(config);
        cubeDesc = cubeMgr.getCube(cubeName).getDescriptor();

        inputCodec = new MeasureCodec(cubeDesc.getMeasures());
        inputMeasures = new Object[cubeDesc.getMeasures().size()];
        keyValueCreators = Lists.newArrayList();

        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new KeyValueCreator(cubeDesc, colDesc));
            }
        }
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        outputKey.set(key.getBytes(), 0, key.getLength());
        KeyValue outputValue;

        int n = keyValueCreators.size();
        if (n == 1 && keyValueCreators.get(0).isFullCopy) { // shortcut for
                                                            // simple full copy

            outputValue = keyValueCreators.get(0).create(key, value.getBytes(), 0, value.getLength());
            context.write(outputKey, outputValue);

        } else { // normal (complex) case that distributes measures to multiple
                 // HBase columns

            inputCodec.decode(value, inputMeasures);

            for (int i = 0; i < n; i++) {
                outputValue = keyValueCreators.get(i).create(key, inputMeasures);
                context.write(outputKey, outputValue);
            }
        }
    }

}
