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
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.gridtable.GTRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class HBaseCuboidWriter implements ICuboidWriter {

    private static final Logger logger = LoggerFactory.getLogger(HBaseCuboidWriter.class);

    private static final int BATCH_PUT_THRESHOLD = 10000;

    private final List<KeyValueCreator> keyValueCreators;
    private final int nColumns;
    private final Table hTable;
    private final CubeDesc cubeDesc;
    private final CubeSegment cubeSegment;
    private final Object[] measureValues;

    private List<Put> puts = Lists.newArrayList();
    private AbstractRowKeyEncoder rowKeyEncoder;
    private byte[] keybuf;

    public HBaseCuboidWriter(CubeSegment segment, Table hTable) {
        this.keyValueCreators = Lists.newArrayList();
        this.cubeSegment = segment;
        this.cubeDesc = cubeSegment.getCubeDesc();
        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new KeyValueCreator(cubeDesc, colDesc));
            }
        }
        this.nColumns = keyValueCreators.size();
        this.hTable = hTable;
        this.measureValues = new Object[cubeDesc.getMeasures().size()];
    }

    private byte[] copy(byte[] array, int offset, int length) {
        byte[] result = new byte[length];
        System.arraycopy(array, offset, result, 0, length);
        return result;
    }

    //TODO:shardingonstreaming
    private byte[] createKey(Long cuboidId, GTRecord record) {
        if (rowKeyEncoder == null || rowKeyEncoder.getCuboidID() != cuboidId) {
            rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, Cuboid.findById(cubeDesc, cuboidId));
            keybuf = rowKeyEncoder.createBuf();
        }
        rowKeyEncoder.encode(record, record.getInfo().getPrimaryKey(), keybuf);
        return keybuf;

    }

    @Override
    public void write(long cuboidId, GTRecord record) throws IOException {
        byte[] key = createKey(cuboidId, record);
        final Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        final int nDims = cuboid.getColumns().size();
        final ImmutableBitSet bitSet = new ImmutableBitSet(nDims, nDims + cubeDesc.getMeasures().size());

        for (int i = 0; i < nColumns; i++) {
            final Object[] values = record.getValues(bitSet, measureValues);
            final KeyValue keyValue = keyValueCreators.get(i).create(key, 0, key.length, values);
            final Put put = new Put(copy(key, 0, key.length));
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

    @Override
    public final void flush() throws IOException {
        if (!puts.isEmpty()) {
            long t = System.currentTimeMillis();
            if (hTable != null) {
                hTable.put(puts);
            }
            logger.info("commit total " + puts.size() + " puts, totally cost:" + (System.currentTimeMillis() - t) + "ms");
            puts.clear();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        IOUtils.closeQuietly(hTable);
    }

}
