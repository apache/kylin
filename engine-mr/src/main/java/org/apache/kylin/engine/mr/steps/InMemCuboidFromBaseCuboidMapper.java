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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.inmemcubing.AbstractInMemCubeBuilder;
import org.apache.kylin.cube.inmemcubing.DoggedCubeBuilder;
import org.apache.kylin.cube.inmemcubing.InputConverterUnit;
import org.apache.kylin.cube.inmemcubing.InputConverterUnitForBaseCuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
    protected Future getCubingThreadFuture(Context context, Map<TblColRef, Dictionary<String>> dictionaryMap,
            int reserveMemoryMB, CuboidScheduler cuboidScheduler) {
        AbstractInMemCubeBuilder cubeBuilder = new DoggedCubeBuilder(cuboidScheduler, flatDesc, dictionaryMap);
        cubeBuilder.setReserveMemoryMB(reserveMemoryMB);
        cubeBuilder.setConcurrentThreads(taskThreadCount);

        ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("inmemory-cube-building-from-base-cuboid-mapper-%d").build());
        return executorService.submit(cubeBuilder.buildAsRunnable(queue, inputConverterUnit,
                new MapContextGTRecordWriter(context, cubeDesc, cubeSegment)));
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
