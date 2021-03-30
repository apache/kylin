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
package org.apache.kylin.cube.inmemcubing;

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.cube.util.KeyValueBuilder;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class InputConverterUnitForRawData implements InputConverterUnit<String[]> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(InputConverterUnitForRawData.class);
    
    public static final String[] END_ROW = new String[0];
    public static final String[] CUT_ROW = { "" };

    private final CubeJoinedFlatTableEnrich flatDesc;
    private final MeasureDesc[] measureDescs;
    private final MeasureIngester<?>[] measureIngesters;
    private final int measureCount;
    private final Map<TblColRef, Dictionary<String>> dictionaryMap;
    private final KeyValueBuilder kvBuilder;

    public InputConverterUnitForRawData(CubeDesc cubeDesc, IJoinedFlatTableDesc flatDesc,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.flatDesc = new CubeJoinedFlatTableEnrich(flatDesc, cubeDesc);
        this.measureCount = cubeDesc.getMeasures().size();
        this.measureDescs = cubeDesc.getMeasures().toArray(new MeasureDesc[measureCount]);
        this.measureIngesters = MeasureIngester.create(cubeDesc.getMeasures());
        this.dictionaryMap = dictionaryMap;
        this.kvBuilder = new KeyValueBuilder(this.flatDesc);
    }

    @Override
    public final void convert(String[] row, GTRecord record) {
        Object[] dimensions = kvBuilder.buildKey(row);
        Object[] metricsValues = buildValue(row);
        Object[] recordValues = new Object[dimensions.length + metricsValues.length];
        System.arraycopy(dimensions, 0, recordValues, 0, dimensions.length);
        System.arraycopy(metricsValues, 0, recordValues, dimensions.length, metricsValues.length);
        record.setValues(recordValues);
    }

    @Override
    public boolean ifEnd(String[] currentObject) {
        return currentObject == END_ROW;
    }

    @Override
    public boolean ifCut(String[] currentObject) {
        return currentObject == CUT_ROW;
    }

    @Override
    public String[] getEndRow() {
        return END_ROW;
    }

    @Override
    public String[] getCutRow() {
        return CUT_ROW;
    }

    @Override
    public boolean ifChange() {
        return true;
    }

    private Object[] buildValue(String[] row) {
        Object[] values = new Object[measureCount];
        for (int i = 0; i < measureCount; i++) {
            String[] colValues = kvBuilder.buildValueOf(i, row);
            MeasureDesc measure = measureDescs[i];
            values[i] = measureIngesters[i].valueOf(colValues, measure, dictionaryMap);
        }
        return values;
    }
}
