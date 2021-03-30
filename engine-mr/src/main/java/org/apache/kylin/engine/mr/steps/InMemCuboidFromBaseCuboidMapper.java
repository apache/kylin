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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.inmemcubing.InputConverterUnit;
import org.apache.kylin.cube.inmemcubing.InputConverterUnitForBaseCuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTInfo;

public class InMemCuboidFromBaseCuboidMapper
        extends InMemCuboidMapperBase<Text, Text, ByteArrayWritable, ByteArrayWritable, ByteArray> {
    private static final Log logger = LogFactory.getLog(InMemCuboidFromBaseCuboidMapper.class);

    private ByteBuffer keyValueBuffer;
    private int keyOffset;

    @Override
    protected void doSetup(Mapper.Context context) throws IOException {
        super.doSetup(context);

        long baseCuboid = Cuboid.getBaseCuboidId(cubeDesc);
        GTInfo gtInfo = CubeGridTable.newGTInfo(Cuboid.findForMandatory(cubeDesc, baseCuboid),
                new CubeDimEncMap(cubeDesc, dictionaryMap));
        keyValueBuffer = ByteBuffer.allocate(gtInfo.getMaxRecordLength());
        keyOffset = cubeSegment.getRowKeyPreambleSize();
    }

    @Override
    protected InputConverterUnit<ByteArray> getInputConverterUnit(Context context) {
        String updateShard = context.getConfiguration().get(BatchConstants.CFG_UPDATE_SHARD);
        if (updateShard == null || updateShard.equalsIgnoreCase("false")) {
            return new InputConverterUnitForBaseCuboid(false);
        } else {
            return new InputConverterUnitForBaseCuboid(true);
        }
    }

    @Override
    protected ICuboidWriter getCuboidWriter(Context context) {
        return new MapContextGTRecordWriter(context, cubeDesc, cubeSegment);
    }

    @Override
    protected ByteArray getRecordFromKeyValue(Text key, Text value) {
        keyValueBuffer.clear();
        keyValueBuffer.put(key.getBytes(), keyOffset, key.getBytes().length - keyOffset);
        keyValueBuffer.put(value.getBytes());

        byte[] keyValue = new byte[keyValueBuffer.position()];
        System.arraycopy(keyValueBuffer.array(), 0, keyValue, 0, keyValueBuffer.position());

        return new ByteArray(keyValue);
    }
}
