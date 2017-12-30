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

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class InputConverterUnitForRawData implements InputConverterUnit<String[]> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(InputConverterUnitForRawData.class);
    
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final String[] END_ROW = new String[0];
    public static final String[] CUT_ROW = { "" };

    private final CubeJoinedFlatTableEnrich flatDesc;
    private final MeasureDesc[] measureDescs;
    private final MeasureIngester<?>[] measureIngesters;
    private final int measureCount;
    private final Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected List<byte[]> nullBytes;

    public InputConverterUnitForRawData(CubeDesc cubeDesc, IJoinedFlatTableDesc flatDesc,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.flatDesc = new CubeJoinedFlatTableEnrich(flatDesc, cubeDesc);
        this.measureCount = cubeDesc.getMeasures().size();
        this.measureDescs = cubeDesc.getMeasures().toArray(new MeasureDesc[measureCount]);
        this.measureIngesters = MeasureIngester.create(cubeDesc.getMeasures());
        this.dictionaryMap = dictionaryMap;
        initNullBytes(cubeDesc);
    }

    @Override
    public final void convert(String[] row, GTRecord record) {
        Object[] dimensions = buildKey(row);
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

    private Object[] buildKey(String[] row) {
        int keySize = flatDesc.getRowKeyColumnIndexes().length;
        Object[] key = new Object[keySize];

        for (int i = 0; i < keySize; i++) {
            key[i] = row[flatDesc.getRowKeyColumnIndexes()[i]];
            if (key[i] != null && isNull(Bytes.toBytes((String) key[i]))) {
                key[i] = null;
            }
        }

        return key;
    }

    @Override
    public boolean ifChange() {
        return true;
    }

    private Object[] buildValue(String[] row) {
        Object[] values = new Object[measureCount];
        for (int i = 0; i < measureCount; i++) {
            values[i] = buildValueOf(i, row);
        }
        return values;
    }

    private Object buildValueOf(int idxOfMeasure, String[] row) {
        MeasureDesc measure = measureDescs[idxOfMeasure];
        FunctionDesc function = measure.getFunction();
        int[] colIdxOnFlatTable = flatDesc.getMeasureColumnIndexes()[idxOfMeasure];

        int paramCount = function.getParameterCount();
        String[] inputToMeasure = new String[paramCount];

        // pick up parameter values
        ParameterDesc param = function.getParameter();
        int paramColIdx = 0; // index among parameters of column type
        for (int i = 0; i < paramCount; i++, param = param.getNextParameter()) {
            String value;
            if (function.isCount()) {
                value = "1";
            } else if (param.isColumnType()) {
                value = row[colIdxOnFlatTable[paramColIdx++]];
            } else {
                value = param.getValue();
            }
            inputToMeasure[i] = value;
        }

        return measureIngesters[idxOfMeasure].valueOf(inputToMeasure, measure, dictionaryMap);
    }

    private void initNullBytes(CubeDesc cubeDesc) {
        nullBytes = Lists.newArrayList();
        nullBytes.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullBytes.add(Bytes.toBytes(s));
            }
        }
    }

    private boolean isNull(byte[] v) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, nullByte))
                return true;
        }
        return false;
    }

}
