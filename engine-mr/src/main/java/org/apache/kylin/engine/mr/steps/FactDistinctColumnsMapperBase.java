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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class FactDistinctColumnsMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, LongWritable, Text> {

    protected String cubeName;
    protected CubeInstance cube;
    protected CubeSegment cubeSeg;
    protected CubeDesc cubeDesc;
    protected long baseCuboidId;
    protected List<TblColRef> dictionaryColumns;
    protected ArrayList<Integer> factDictCols;
    protected IMRTableInputFormat flatTableInputFormat;

    protected LongWritable outputKey = new LongWritable();
    protected Text outputValue = new Text();
    protected int errorRecordCounter = 0;

    protected CubeJoinedFlatTableDesc intermediateTableDesc;
    protected int[] dictionaryColumnIndex;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeSeg = cube.getSegment(conf.get(BatchConstants.CFG_CUBE_SEGMENT_NAME), SegmentStatusEnum.NEW);
        cubeDesc = cube.getDescriptor();
        baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        dictionaryColumns = cubeDesc.getAllColumnsNeedDictionary();

        factDictCols = new ArrayList<Integer>();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        for (int i = 0; i < dictionaryColumns.size(); i++) {
            TblColRef col = dictionaryColumns.get(i);

            String scanTable = dictMgr.decideSourceData(cubeDesc.getModel(), "true", col).getTable();
            if (cubeDesc.getModel().isFactTable(scanTable)) {
                factDictCols.add(i);
            }
        }

        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();

        intermediateTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
        dictionaryColumnIndex = new int[factDictCols.size()];
        for (int i = 0; i < factDictCols.size(); i++) {
            Integer column = factDictCols.get(i);
            TblColRef colRef = dictionaryColumns.get(column);
            int columnIndexOnFlatTbl = intermediateTableDesc.getColumnIndex(colRef);
            dictionaryColumnIndex[i] = columnIndexOnFlatTbl;
        }

    }

    protected void handleErrorRecord(String[] record, Exception ex) throws IOException {

        System.err.println("Insane record: " + Arrays.toString(record));
        ex.printStackTrace(System.err);

        errorRecordCounter++;
        if (errorRecordCounter > BatchConstants.ERROR_RECORD_THRESHOLD) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
