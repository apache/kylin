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

package org.apache.kylin.engine.mr.common;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 */
public class BaseCuboidBuilder implements java.io.Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(BaseCuboidBuilder.class);
    public static final String HIVE_NULL = "\\N";
    protected String cubeName;
    protected Cuboid baseCuboid;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected Set<String> nullStrs;
    protected CubeJoinedFlatTableEnrich intermediateTableDesc;
    protected MeasureIngester<?>[] aggrIngesters;
    protected Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected AbstractRowKeyEncoder rowKeyEncoder;
    protected BufferedMeasureCodec measureCodec;

    protected KylinConfig kylinConfig;

    public BaseCuboidBuilder(KylinConfig kylinConfig, CubeDesc cubeDesc, CubeSegment cubeSegment, CubeJoinedFlatTableEnrich intermediateTableDesc,
                             AbstractRowKeyEncoder rowKeyEncoder, MeasureIngester<?>[] aggrIngesters, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.kylinConfig = kylinConfig;
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.intermediateTableDesc = intermediateTableDesc;
        this.rowKeyEncoder = rowKeyEncoder;
        this.aggrIngesters = aggrIngesters;
        this.dictionaryMap = dictionaryMap;

        init();
        measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
    }

    public BaseCuboidBuilder(KylinConfig kylinConfig, CubeDesc cubeDesc, CubeSegment cubeSegment, CubeJoinedFlatTableEnrich intermediateTableDesc) {
        this.kylinConfig = kylinConfig;
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.intermediateTableDesc = intermediateTableDesc;

        init();
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);
        measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
        aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());
        dictionaryMap = cubeSegment.buildDictionaryMap();

    }

    private void init() {
        baseCuboid = Cuboid.getBaseCuboid(cubeDesc);
        initNullBytes();
    }

    private void initNullBytes() {
        nullStrs = Sets.newHashSet();
        nullStrs.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullStrs.add(s);
            }
        }
    }

    protected boolean isNull(String v) {
        return nullStrs.contains(v);
    }

    public byte[] buildKey(String[] flatRow) {
        int[] rowKeyColumnIndexes = intermediateTableDesc.getRowKeyColumnIndexes();
        List<TblColRef> columns = baseCuboid.getColumns();
        String[] colValues = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            colValues[i] = getCell(rowKeyColumnIndexes[i], flatRow);
        }
        return rowKeyEncoder.encode(colValues);
    }

    public ByteBuffer buildValue(String[] flatRow) {
        return measureCodec.encode(buildValueObjects(flatRow));
    }

    public Object[] buildValueObjects(String[] flatRow) {
        Object[] measures = new Object[cubeDesc.getMeasures().size()];
        for (int i = 0; i < measures.length; i++) {
            measures[i] = buildValueOf(i, flatRow);
        }

        return measures;
    }

    public void resetAggrs() {
        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            aggrIngesters[i].reset();
        }
    }

    private Object buildValueOf(int idxOfMeasure, String[] flatRow) {
        MeasureDesc measure = cubeDesc.getMeasures().get(idxOfMeasure);
        FunctionDesc function = measure.getFunction();
        int[] colIdxOnFlatTable = intermediateTableDesc.getMeasureColumnIndexes()[idxOfMeasure];

        int paramCount = function.getParameterCount();
        String[] inputToMeasure = new String[paramCount];

        // pick up parameter values
        ParameterDesc param = function.getParameter();
        int colParamIdx = 0; // index among parameters of column type
        for (int i = 0; i < paramCount; i++, param = param.getNextParameter()) {
            String value;
            if (function.isCount()) {
                value = "1";
            } else if (param.isColumnType()) {
                value = getCell(colIdxOnFlatTable[colParamIdx++], flatRow);
            } else {
                value = param.getValue();
            }
            inputToMeasure[i] = value;
        }

        return aggrIngesters[idxOfMeasure].valueOf(inputToMeasure, measure, dictionaryMap);
    }

    private String getCell(int i, String[] flatRow) {
        if (isNull(flatRow[i]))
            return null;
        else
            return flatRow[i];
    }

}
