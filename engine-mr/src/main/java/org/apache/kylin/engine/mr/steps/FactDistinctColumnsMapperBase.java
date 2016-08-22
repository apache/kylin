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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;

/**
 */
public class FactDistinctColumnsMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {

    protected String cubeName;
    protected CubeInstance cube;
    protected CubeSegment cubeSeg;
    protected CubeDesc cubeDesc;
    protected long baseCuboidId;
    protected List<TblColRef> factDictCols;
    protected IMRTableInputFormat flatTableInputFormat;

    protected Text outputKey = new Text();
    protected Text outputValue = new Text();
    protected int errorRecordCounter = 0;

    protected CubeJoinedFlatTableEnrich intermediateTableDesc;
    protected int[] dictionaryColumnIndex;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeSeg = cube.getSegmentById(conf.get(BatchConstants.CFG_CUBE_SEGMENT_ID));
        cubeDesc = cube.getDescriptor();
        baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        factDictCols = CubeManager.getInstance(config).getAllDictColumnsOnFact(cubeDesc);

        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();

        intermediateTableDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSeg),  cubeDesc);
        dictionaryColumnIndex = new int[factDictCols.size()];
        for (int i = 0; i < factDictCols.size(); i++) {
            TblColRef colRef = factDictCols.get(i);
            int columnIndexOnFlatTbl = intermediateTableDesc.getColumnIndex(colRef);
            dictionaryColumnIndex[i] = columnIndexOnFlatTbl;
        }

    }

    protected void handleErrorRecord(String[] record, Exception ex) throws IOException {

        System.err.println("Insane record: " + Arrays.toString(record));
        ex.printStackTrace(System.err);

        errorRecordCounter++;
        if (errorRecordCounter > BatchConstants.ERROR_RECORD_LOG_THRESHOLD) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
