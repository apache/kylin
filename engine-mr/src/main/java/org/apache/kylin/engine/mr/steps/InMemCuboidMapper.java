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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.inmemcubing.AbstractInMemCubeBuilder;
import org.apache.kylin.cube.inmemcubing.DoggedCubeBuilder;
import org.apache.kylin.cube.inmemcubing.InputConverterUnit;
import org.apache.kylin.cube.inmemcubing.InputConverterUnitForRawData;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class InMemCuboidMapper<KEYIN>
        extends InMemCuboidMapperBase<KEYIN, Object, ByteArrayWritable, ByteArrayWritable, String[]> {

    private IMRInput.IMRTableInputFormat flatTableInputFormat;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.doSetup(context);

        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();
    }

    @Override
    protected InputConverterUnit<String[]> getInputConverterUnit(Context context) {
        Preconditions.checkNotNull(cubeDesc);
        Preconditions.checkNotNull(dictionaryMap);
        return new InputConverterUnitForRawData(cubeDesc, flatDesc, dictionaryMap);
    }

    @Override
    protected String[] getRecordFromKeyValue(KEYIN key, Object value) {
        return flatTableInputFormat.parseMapperInput(value).iterator().next();
    }

    @Override
    protected Future getCubingThreadFuture(Context context, Map<TblColRef, Dictionary<String>> dictionaryMap,
            int reserveMemoryMB, CuboidScheduler cuboidScheduler) {
        AbstractInMemCubeBuilder cubeBuilder = new DoggedCubeBuilder(cuboidScheduler, flatDesc, dictionaryMap);
        cubeBuilder.setReserveMemoryMB(reserveMemoryMB);
        cubeBuilder.setConcurrentThreads(taskThreadCount);

        ExecutorService executorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("inmemory-cube-building-mapper-%d").build());
        return executorService.submit(cubeBuilder.buildAsRunnable(queue, inputConverterUnit,
                new MapContextGTRecordWriter(context, cubeDesc, cubeSegment)));
    }
}
