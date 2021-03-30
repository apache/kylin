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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidUtil;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hasher;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;

public class CalculateStatsFromBaseCuboidMapper extends KylinMapper<Text, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(CalculateStatsFromBaseCuboidMapper.class);

    protected int nRowKey;
    protected long baseCuboidId;

    private int samplingPercentage;
    private int rowCount = 0;
    private long[] rowHashCodesLong = null;
    //about details of the new algorithm, please see KYLIN-2518
    private boolean isUsePutRowKeyToHllNewAlgorithm;

    private HLLCounter[] allCuboidsHLL = null;
    private Long[] cuboidIds;
    private Integer[][] allCuboidsBitSet = null;
    private HashFunction hf = null;

    RowKeyDecoder rowKeyDecoder;

    protected Text outputKey = new Text();
    protected Text outputValue = new Text();

    @Override
    protected void doSetup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        HadoopUtil.setCurrentConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeSegment cubeSegment = cube.getSegmentById(conf.get(BatchConstants.CFG_CUBE_SEGMENT_ID));

        baseCuboidId = cube.getCuboidScheduler().getBaseCuboidId();
        nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;

        String cuboidModeName = conf.get(BatchConstants.CFG_CUBOID_MODE);
        Set<Long> cuboidIdSet = cube.getCuboidsByMode(cuboidModeName);

        cuboidIds = cuboidIdSet.toArray(new Long[cuboidIdSet.size()]);
        allCuboidsBitSet = CuboidUtil.getCuboidBitSet(cuboidIds, nRowKey);

        samplingPercentage = Integer
                .parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT));

        allCuboidsHLL = new HLLCounter[cuboidIds.length];
        for (int i = 0; i < cuboidIds.length; i++) {
            allCuboidsHLL[i] = new HLLCounter(cubeDesc.getConfig().getCubeStatsHLLPrecision());
        }

        //for KYLIN-2518 backward compatibility
        if (KylinVersion.isBefore200(cubeDesc.getVersion())) {
            isUsePutRowKeyToHllNewAlgorithm = false;
            hf = Hashing.murmur3_32();
            logger.info("Found KylinVersion : {}. Use old algorithm for cuboid sampling.", cubeDesc.getVersion());
        } else {
            isUsePutRowKeyToHllNewAlgorithm = true;
            rowHashCodesLong = new long[nRowKey];
            hf = Hashing.murmur3_128();
            logger.info(
                    "Found KylinVersion : {}. Use new algorithm for cuboid sampling. About the details of the new algorithm, please refer to KYLIN-2518",
                    cubeDesc.getVersion());
        }

        rowKeyDecoder = new RowKeyDecoder(cubeSegment);
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws InterruptedException, IOException {
        long cuboidID = rowKeyDecoder.decode(key.getBytes());
        if (cuboidID != baseCuboidId) {
            return; // Skip data from cuboids which are not the base cuboid
        }

        List<String> keyValues = rowKeyDecoder.getValues();

        if (rowCount < samplingPercentage) {
            Preconditions.checkArgument(nRowKey == keyValues.size());

            String[] row = keyValues.toArray(new String[keyValues.size()]);
            if (isUsePutRowKeyToHllNewAlgorithm) {
                putRowKeyToHLLNew(row);
            } else {
                putRowKeyToHLLOld(row);
            }
        }

        if (++rowCount == 100)
            rowCount = 0;
    }

    public void putRowKeyToHLLOld(String[] row) {
        //generate hash for each row key column
        byte[][] rowHashCodes = new byte[nRowKey][];
        for (int i = 0; i < nRowKey; i++) {
            Hasher hc = hf.newHasher();
            String colValue = row[i];
            if (colValue != null) {
                rowHashCodes[i] = hc.putUnencodedChars(colValue).hash().asBytes();
            } else {
                rowHashCodes[i] = hc.putInt(0).hash().asBytes();
            }
        }

        // use the row key column hash to get a consolidated hash for each cuboid
        for (int i = 0; i < cuboidIds.length; i++) {
            Hasher hc = hf.newHasher();
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                hc.putBytes(rowHashCodes[allCuboidsBitSet[i][position]]);
            }

            allCuboidsHLL[i].add(hc.hash().asBytes());
        }
    }

    private void putRowKeyToHLLNew(String[] row) {
        //generate hash for each row key column
        for (int i = 0; i < nRowKey; i++) {
            Hasher hc = hf.newHasher();
            String colValue = row[i];
            if (colValue == null)
                colValue = "0";
            byte[] bytes = hc.putUnencodedChars(colValue).hash().asBytes();
            rowHashCodesLong[i] = (Bytes.toLong(bytes) + i);//add column ordinal to the hash value to distinguish between (a,b) and (b,a)
        }

        // user the row key column hash to get a consolidated hash for each cuboid
        for (int i = 0, n = allCuboidsBitSet.length; i < n; i++) {
            long value = 0;
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                value += rowHashCodesLong[allCuboidsBitSet[i][position]];
            }
            allCuboidsHLL[i].addHashDirectly(value);
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
        HLLCounter hll;
        for (int i = 0; i < cuboidIds.length; i++) {
            hll = allCuboidsHLL[i];

            outputKey.set(Bytes.toBytes(cuboidIds[i]));
            hllBuf.clear();
            hll.writeRegisters(hllBuf);
            outputValue.set(hllBuf.array(), 0, hllBuf.position());
            context.write(outputKey, outputValue);
        }
    }
}
