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
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BaseCuboidBuilder;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.DictionaryGetterUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
abstract public class BaseCuboidMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(BaseCuboidMapperBase.class);
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String cubeName;
    protected String segmentID;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected int counter;
    protected Object[] measures;
    private int errorRecordCounter;
    protected Text outputKey = new Text();
    protected Text outputValue = new Text();

    private BaseCuboidBuilder baseCuboidBuilder;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        final KylinConfig kylinConfig = AbstractHadoopJob.loadKylinPropsAndMetadata();
        cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegmentById(segmentID);
        CubeJoinedFlatTableEnrich intermediateTableDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);

        Map<TblColRef, Dictionary<String>> dictionaryMap = DictionaryGetterUtil.getDictionaryMap(cubeSegment,
                context.getInputSplit(), context.getConfiguration());

        baseCuboidBuilder = new BaseCuboidBuilder(kylinConfig, cubeDesc, cubeSegment, intermediateTableDesc,
                dictionaryMap);
    }


    protected void outputKV(String[] flatRow, Context context) throws IOException, InterruptedException {
        byte[] rowKey = baseCuboidBuilder.buildKey(flatRow);
        outputKey.set(rowKey, 0, rowKey.length);

        ByteBuffer valueBuf = baseCuboidBuilder.buildValue(flatRow);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(outputKey, outputValue);
    }

    protected void handleErrorRecord(String[] flatRow, Exception ex) throws IOException {

        logger.error("Insane record: " + Arrays.toString(flatRow), ex);

        // TODO expose errorRecordCounter as hadoop counter
        errorRecordCounter++;
        if (errorRecordCounter > cubeSegment.getConfig().getErrorRecordThreshold()) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
