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

package org.apache.kylin.cube.inmemcubing;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An interface alike abstract class. Hold common tunable parameters and nothing more.
 */
abstract public class AbstractInMemCubeBuilder {

    private static Logger logger = LoggerFactory.getLogger(AbstractInMemCubeBuilder.class);

    final protected CuboidScheduler cuboidScheduler;
    final protected IJoinedFlatTableDesc flatDesc;
    final protected CubeDesc cubeDesc;
    final protected Map<TblColRef, Dictionary<String>> dictionaryMap;

    protected int taskThreadCount = 1;
    protected int reserveMemoryMB = 100;

    protected AbstractInMemCubeBuilder(CuboidScheduler cuboidScheduler, IJoinedFlatTableDesc flatDesc,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        if (cuboidScheduler == null)
            throw new NullPointerException();
        if (flatDesc == null)
            throw new NullPointerException();
        if (dictionaryMap == null)
            throw new IllegalArgumentException("dictionary cannot be null");

        this.cuboidScheduler = cuboidScheduler;
        this.flatDesc = flatDesc;
        this.cubeDesc = cuboidScheduler.getCubeDesc();
        this.dictionaryMap = dictionaryMap;
    }

    public void setConcurrentThreads(int n) {
        this.taskThreadCount = n;
    }

    public void setReserveMemoryMB(int mb) {
        this.reserveMemoryMB = mb;
    }

    public int getReserveMemoryMB() {
        return this.reserveMemoryMB;
    }

    public Runnable buildAsRunnable(final BlockingQueue<String[]> input, final ICuboidWriter output) {
        return buildAsRunnable(input, new InputConverterUnitForRawData(cubeDesc, flatDesc, dictionaryMap), output);
    }

    public <T> Runnable buildAsRunnable(final BlockingQueue<T> input, final InputConverterUnit<T> inputConverterUnit,
            final ICuboidWriter output) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    build(input, inputConverterUnit, output);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    abstract public <T> void build(BlockingQueue<T> input, InputConverterUnit<T> inputConverterUnit,
            ICuboidWriter output) throws IOException;

    protected void outputCuboid(long cuboidId, GridTable gridTable, ICuboidWriter output) throws IOException {
        long startTime = System.currentTimeMillis();
        GTScanRequest req = new GTScanRequestBuilder().setInfo(gridTable.getInfo()).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest();
        try (IGTScanner scanner = gridTable.scan(req)) {
            for (GTRecord record : scanner) {
                output.write(cuboidId, record);
            }
        }
        logger.debug("Cuboid " + cuboidId + " output takes " + (System.currentTimeMillis() - startTime) + "ms");
    }

}
