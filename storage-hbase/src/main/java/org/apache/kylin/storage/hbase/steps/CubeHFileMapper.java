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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * @author George Song (ysong1)
 * 
 */
public class CubeHFileMapper extends KylinMapper<Text, Text, RowKeyWritable, KeyValue> {

    String cubeName;
    CubeDesc cubeDesc;

    MeasureCodec inputCodec;
    Object[] inputMeasures;
    List<KeyValueCreator> keyValueCreators;

    private RowKeyWritable rowKeyWritable = new RowKeyWritable();

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeManager cubeMgr = CubeManager.getInstance(config);
        cubeDesc = cubeMgr.getCube(cubeName).getDescriptor();

        inputCodec = new MeasureCodec(cubeDesc.getMeasures());
        inputMeasures = new Object[cubeDesc.getMeasures().size()];
        keyValueCreators = Lists.newArrayList();

        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new KeyValueCreator(cubeDesc, colDesc));
            }
        }
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        KeyValue outputValue;

        int n = keyValueCreators.size();
        if (n == 1 && keyValueCreators.get(0).isFullCopy) { // shortcut for simple full copy

            outputValue = keyValueCreators.get(0).create(key, value.getBytes(), 0, value.getLength());
            rowKeyWritable.set(outputValue.createKeyOnly(false).getKey());
            context.write(rowKeyWritable, outputValue);

        } else { // normal (complex) case that distributes measures to multiple HBase columns

            inputCodec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), inputMeasures);

            for (int i = 0; i < n; i++) {
                outputValue = keyValueCreators.get(i).create(key, inputMeasures);
                rowKeyWritable.set(outputValue.createKeyOnly(false).getKey());
                context.write(rowKeyWritable, outputValue);
            }
        }
    }

}
