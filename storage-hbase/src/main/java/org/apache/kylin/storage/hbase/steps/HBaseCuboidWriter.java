/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */
package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.gridtable.GTRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public final class HBaseCuboidWriter implements ICuboidWriter {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStreamingOutput.class);

    private static final int BATCH_PUT_THRESHOLD = 10000;

    private final List<KeyValueCreator> keyValueCreators;
    private final int nColumns;
    private final HTableInterface hTable;
    private final ByteBuffer byteBuffer;
    private final CubeDesc cubeDesc;
    private final Object[] measureValues;
    private List<Put> puts = Lists.newArrayList();

    public HBaseCuboidWriter(CubeDesc cubeDesc, HTableInterface hTable) {
        this.keyValueCreators = Lists.newArrayList();
        this.cubeDesc = cubeDesc;
        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new KeyValueCreator(cubeDesc, colDesc));
            }
        }
        this.nColumns = keyValueCreators.size();
        this.hTable = hTable;
        this.byteBuffer = ByteBuffer.allocate(1 << 20);
        this.measureValues = new Object[cubeDesc.getMeasures().size()];
    }

    private byte[] copy(byte[] array, int offset, int length) {
        byte[] result = new byte[length];
        System.arraycopy(array, offset, result, 0, length);
        return result;
    }

    private ByteBuffer createKey(Long cuboidId, GTRecord record) {
        byteBuffer.clear();
        byteBuffer.put(Bytes.toBytes(cuboidId));
        final int cardinality = BitSet.valueOf(new long[] { cuboidId }).cardinality();
        for (int i = 0; i < cardinality; i++) {
            final ByteArray byteArray = record.get(i);
            byteBuffer.put(byteArray.array(), byteArray.offset(), byteArray.length());
        }
        return byteBuffer;
    }

    @Override
    public void write(long cuboidId, GTRecord record) throws IOException {
        final ByteBuffer key = createKey(cuboidId, record);
        final Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        final int nDims = cuboid.getColumns().size();
        final ImmutableBitSet bitSet = new ImmutableBitSet(nDims, nDims + cubeDesc.getMeasures().size());
        
        for (int i = 0; i < nColumns; i++) {
            final Object[] values = record.getValues(bitSet, measureValues);
            final KeyValue keyValue = keyValueCreators.get(i).create(key.array(), 0, key.position(), values);
            final Put put = new Put(copy(key.array(), 0, key.position()));
            byte[] family = copy(keyValue.getFamilyArray(), keyValue.getFamilyOffset(), keyValue.getFamilyLength());
            byte[] qualifier = copy(keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength());
            byte[] value = copy(keyValue.getValueArray(), keyValue.getValueOffset(), keyValue.getValueLength());
            put.add(family, qualifier, value);
            puts.add(put);
        }
        if (puts.size() >= BATCH_PUT_THRESHOLD) {
            flush();
        }
    }

    public final void flush() {
        try {
            if (!puts.isEmpty()) {
                long t = System.currentTimeMillis();
                if (hTable != null) {
                    hTable.put(puts);
                    hTable.flushCommits();
                }
                logger.info("commit total " + puts.size() + " puts, totally cost:" + (System.currentTimeMillis() - t) + "ms");
                puts.clear();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
