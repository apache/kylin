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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.inmemcubing.ICuboidGTTableWriter;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public abstract class KVGTRecordWriter extends ICuboidGTTableWriter {

    private static final Logger logger = LoggerFactory.getLogger(KVGTRecordWriter.class);

    private Long lastCuboidId;
    protected CubeSegment cubeSegment;
    protected CubeDesc cubeDesc;

    private AbstractRowKeyEncoder rowKeyEncoder;
    private int dimensions;
    private int measureCount;
    private byte[] keyBuf;
    private ImmutableBitSet measureColumns;
    private ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
    private ByteArrayWritable outputKey = new ByteArrayWritable();
    private ByteArrayWritable outputValue = new ByteArrayWritable();
    private long cuboidRowCount = 0;

    //for shard

    public KVGTRecordWriter(CubeDesc cubeDesc, CubeSegment cubeSegment) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.measureCount = cubeDesc.getMeasures().size();
    }

    @Override
    public void write(long cuboidId, GTRecord record) throws IOException {

        if (lastCuboidId == null || !lastCuboidId.equals(cuboidId)) {
            if (lastCuboidId != null) {
                logger.info("Cuboid " + lastCuboidId + " has " + cuboidRowCount + " rows");
                cuboidRowCount = 0;
            }
            // output another cuboid
            initVariables(cuboidId);
            lastCuboidId = cuboidId;
        }

        cuboidRowCount++;
        rowKeyEncoder.encode(record, record.getInfo().getPrimaryKey(), keyBuf);

        //output measures
        valueBuf.clear();
        try {
            record.exportColumns(measureColumns, valueBuf);
        } catch (BufferOverflowException boe) {
            valueBuf = ByteBuffer.allocate((int) (record.sizeOf(measureColumns) * 1.5));
            record.exportColumns(measureColumns, valueBuf);
        }

        outputKey.set(keyBuf, 0, keyBuf.length);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        writeAsKeyValue(outputKey, outputValue);
    }

    protected abstract void writeAsKeyValue(ByteArrayWritable key, ByteArrayWritable value) throws IOException;

    private void initVariables(Long cuboidId) {
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, Cuboid.findForMandatory(cubeDesc, cuboidId));
        keyBuf = rowKeyEncoder.createBuf();

        dimensions = Long.bitCount(cuboidId);
        measureColumns = new ImmutableBitSet(dimensions, dimensions + measureCount);
    }
}
