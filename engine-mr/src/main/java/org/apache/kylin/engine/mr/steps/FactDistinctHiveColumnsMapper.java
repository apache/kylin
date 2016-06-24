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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 */
public class FactDistinctHiveColumnsMapper<KEYIN> extends FactDistinctColumnsMapperBase<KEYIN, Object> {

    protected boolean collectStatistics = false;
    protected CuboidScheduler cuboidScheduler = null;
    protected int nRowKey;
    private Integer[][] allCuboidsBitSet = null;
    private HyperLogLogPlusCounter[] allCuboidsHLL = null;
    private Long[] cuboidIds;
    private HashFunction hf = null;
    private int rowCount = 0;
    private int samplingPercentage;
    private ByteArray[] row_hashcodes = null;
    private ByteBuffer keyBuffer;
    private static final Text EMPTY_TEXT = new Text();
    public static final byte MARK_FOR_HLL = (byte) 0xFF;

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);
        keyBuffer = ByteBuffer.allocate(4096);
        collectStatistics = Boolean.parseBoolean(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_ENABLED));
        if (collectStatistics) {
            samplingPercentage = Integer.parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT));
            cuboidScheduler = new CuboidScheduler(cubeDesc);
            nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;

            List<Long> cuboidIdList = Lists.newArrayList();
            List<Integer[]> allCuboidsBitSetList = Lists.newArrayList();
            addCuboidBitSet(baseCuboidId, allCuboidsBitSetList, cuboidIdList);

            allCuboidsBitSet = allCuboidsBitSetList.toArray(new Integer[cuboidIdList.size()][]);
            cuboidIds = cuboidIdList.toArray(new Long[cuboidIdList.size()]);

            allCuboidsHLL = new HyperLogLogPlusCounter[cuboidIds.length];
            for (int i = 0; i < cuboidIds.length; i++) {
                allCuboidsHLL[i] = new HyperLogLogPlusCounter(cubeDesc.getConfig().getCubeStatsHLLPrecision());
            }

            hf = Hashing.murmur3_32();
            row_hashcodes = new ByteArray[nRowKey];
            for (int i = 0; i < nRowKey; i++) {
                row_hashcodes[i] = new ByteArray();
            }
        }
    }

    private void addCuboidBitSet(long cuboidId, List<Integer[]> allCuboidsBitSet, List<Long> allCuboids) {
        allCuboids.add(cuboidId);
        Integer[] indice = new Integer[Long.bitCount(cuboidId)];

        long mask = Long.highestOneBit(baseCuboidId);
        int position = 0;
        for (int i = 0; i < nRowKey; i++) {
            if ((mask & cuboidId) > 0) {
                indice[position] = i;
                position++;
            }
            mask = mask >> 1;
        }

        allCuboidsBitSet.add(indice);
        Collection<Long> children = cuboidScheduler.getSpanningCuboid(cuboidId);
        for (Long childId : children) {
            addCuboidBitSet(childId, allCuboidsBitSet, allCuboids);
        }
    }

    @Override
    public void map(KEYIN key, Object record, Context context) throws IOException, InterruptedException {
        String[] row = flatTableInputFormat.parseMapperInput(record);
        try {
            for (int i = 0; i < factDictCols.size(); i++) {
                String fieldValue = row[dictionaryColumnIndex[i]];
                if (fieldValue == null)
                    continue;

                keyBuffer.clear();
                keyBuffer.put(Bytes.toBytes(i)[3]); // one byte is enough
                keyBuffer.put(Bytes.toBytes(fieldValue));
                outputKey.set(keyBuffer.array(), 0, keyBuffer.position());
                context.write(outputKey, EMPTY_TEXT);
            }
        } catch (Exception ex) {
            handleErrorRecord(row, ex);
        }

        if (collectStatistics && rowCount < samplingPercentage) {
            putRowKeyToHLL(row);
        }

        if (rowCount++ == 100)
            rowCount = 0;
    }

    private void putRowKeyToHLL(String[] row) {

        //generate hash for each row key column
        for (int i = 0; i < nRowKey; i++) {
            Hasher hc = hf.newHasher();
            String colValue = row[intermediateTableDesc.getRowKeyColumnIndexes()[i]];
            if (colValue != null) {
                row_hashcodes[i].set(hc.putString(colValue).hash().asBytes());
            } else {
                row_hashcodes[i].set(hc.putInt(0).hash().asBytes());
            }
        }

        // user the row key column hash to get a consolidated hash for each cuboid
        for (int i = 0, n = allCuboidsBitSet.length; i < n; i++) {
            Hasher hc = hf.newHasher();
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                hc.putBytes(row_hashcodes[allCuboidsBitSet[i][position]].array());
            }

            allCuboidsHLL[i].add(hc.hash().asBytes());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (collectStatistics) {
            ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);
            // output each cuboid's hll to reducer, key is 0 - cuboidId
            HyperLogLogPlusCounter hll;
            for (int i = 0; i < cuboidIds.length; i++) {
                hll = allCuboidsHLL[i];

                keyBuffer.clear();
                keyBuffer.put(MARK_FOR_HLL); // one byte
                keyBuffer.putLong(cuboidIds[i]);
                outputKey.set(keyBuffer.array(), 0, keyBuffer.position());
                hllBuf.clear();
                hll.writeRegisters(hllBuf);
                outputValue.set(hllBuf.array(), 0, hllBuf.position());
                context.write(outputKey, outputValue);
            }
        }
    }
}
