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
import java.util.Collections;
import java.util.HashSet;
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
import org.apache.kylin.cube.util.KeyValueBuilder;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@SuppressWarnings("serial")
public class BaseCuboidBuilder implements java.io.Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(BaseCuboidBuilder.class);
    protected KylinConfig kylinConfig;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected CubeJoinedFlatTableEnrich intermediateTableDesc;
    protected MeasureIngester<?>[] aggrIngesters;
    protected Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected AbstractRowKeyEncoder rowKeyEncoder;
    protected List<MeasureDesc> measureDescList;
    protected BufferedMeasureCodec measureCodec;
    protected KeyValueBuilder kvBuilder;

    public BaseCuboidBuilder(KylinConfig kylinConfig, CubeDesc cubeDesc, CubeSegment cubeSegment,
            CubeJoinedFlatTableEnrich intermediateTableDesc, AbstractRowKeyEncoder rowKeyEncoder,
            MeasureIngester<?>[] aggrIngesters, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.kylinConfig = kylinConfig;
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.intermediateTableDesc = intermediateTableDesc;
        this.dictionaryMap = dictionaryMap;
        this.rowKeyEncoder = rowKeyEncoder;
        this.aggrIngesters = aggrIngesters;

        measureDescList = cubeDesc.getMeasures();
        measureCodec = new BufferedMeasureCodec(measureDescList);

        kvBuilder = new KeyValueBuilder(intermediateTableDesc);
        checkMrDictClolumn();
    }

    public BaseCuboidBuilder(KylinConfig kylinConfig, CubeDesc cubeDesc, CubeSegment cubeSegment,
            CubeJoinedFlatTableEnrich intermediateTableDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.kylinConfig = kylinConfig;
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.intermediateTableDesc = intermediateTableDesc;
        this.dictionaryMap = dictionaryMap;

        Cuboid baseCuboid = Cuboid.getBaseCuboid(cubeDesc);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureDescList = cubeDesc.getMeasures();
        aggrIngesters = MeasureIngester.create(measureDescList);
        measureCodec = new BufferedMeasureCodec(measureDescList);

        kvBuilder = new KeyValueBuilder(intermediateTableDesc);
        checkMrDictClolumn();
    }

    public byte[] buildKey(String[] flatRow) {
        String[] colKeys = kvBuilder.buildKey(flatRow);
        return rowKeyEncoder.encode(colKeys);
    }

    public ByteBuffer buildValue(String[] flatRow) {
        return measureCodec.encode(buildValueObjects(flatRow));
    }

    public Object[] buildValueObjects(String[] flatRow) {
        Object[] measures = new Object[cubeDesc.getMeasures().size()];

        for (int i = 0; i < measures.length; i++) {
            String[] colValues = kvBuilder.buildValueOf(i, flatRow);
            MeasureDesc measure = measureDescList.get(i);
            measures[i] = aggrIngesters[i].valueOf(colValues, measure, dictionaryMap);
        }
        return measures;
    }

    public void resetAggrs() {
        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            aggrIngesters[i].reset();
        }
    }

    private void checkMrDictClolumn(){
        Set<String> mrDictColumnSet = new HashSet<>();
        if (kylinConfig.getMrHiveDictColumns() != null) {
            Collections.addAll(mrDictColumnSet, kylinConfig.getMrHiveDictColumns());
        }

        for (MeasureDesc measure : measureDescList) {
            if (measure.getFunction().getExpression().equalsIgnoreCase(FunctionDesc.FUNC_COUNT_DISTINCT)) {
                FunctionDesc functionDesc = measure.getFunction();
                TblColRef colRef = functionDesc.getParameter().getColRefs().get(0);
                if (mrDictColumnSet.contains(JoinedFlatTable.colName(colRef, true))) {
                    functionDesc.setMrDict(true);
                    logger.info("setMrDict for {}", colRef);
                    measure.setFunction(functionDesc);
                }
            }
        }
    }
}
