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
package org.apache.kylin.metadata.measure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.common.util.Unsafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class TopNMeasureTypeTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setup() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void clear() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testIngester() throws Exception {
        FunctionDesc functionDesc = initFunction();

        MeasureDesc measureDesc = new MeasureDesc();
        measureDesc.setFunction(functionDesc);
        TopNMeasureType measureType = (TopNMeasureType) MeasureTypeFactory.create(functionDesc.getExpression(),
                functionDesc.getReturnDataType());
        Map<TblColRef, Dictionary<String>> dictionaryMap = initDictionary();

        MeasureIngester<TopNCounter<ByteArray>> ingester = measureType.newIngester();

        TopNCounter topNCounter;
        topNCounter = ingester.valueOf(new String[] { "1", "2" }, measureDesc, dictionaryMap);
        assertEquals(5000, topNCounter.getCapacity());

        String returnType = measureDesc.getFunction().getReturnType();
        measureType.fixMeasureReturnType(measureDesc);
        assertTrue(measureDesc.getFunction().getReturnType().equals("topn(100,4)"));
        assertTrue(!measureDesc.getFunction().getReturnType().equals(returnType));

        TblColRef sellerColRef = functionDesc.getColRefs().get(1);
        functionDesc.getConfiguration().put(TopNMeasureType.CONFIG_ENCODING_PREFIX + sellerColRef.getIdentity(),
                "int:6");
        measureType.fixMeasureReturnType(measureDesc);
        assertTrue(measureDesc.getFunction().getReturnType().equals("topn(100,6)"));
        assertTrue(!measureDesc.getFunction().getReturnType().equals(returnType));

    }

    private Map<TblColRef, Dictionary<String>> initDictionary() {
        Map<TblColRef, Dictionary<String>> dictionaryMap = Maps.newHashMap();
        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "SELLER_ID", "long");
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "TOTAL_PRICE", "double");
        MockDictionary mockDict = new MockDictionary();
        mockDict.values = new String[] { "1", "2", "3" };
        dictionaryMap.put(col1, mockDict);
        dictionaryMap.put(col2, mockDict);

        return dictionaryMap;
    }

    @Test
    public void testTopNMeasureType() throws Exception {

        FunctionDesc functionDesc = initFunction();

        TopNMeasureType measureType = (TopNMeasureType) MeasureTypeFactory.create(functionDesc.getExpression(),
                functionDesc.getReturnDataType());

        functionDesc.getConfiguration().clear();
        List<TblColRef> colsNeedDict = measureType.getColumnsNeedDictionary(functionDesc);

        assertTrue(colsNeedDict != null && colsNeedDict.size() == 1);

        TblColRef sellerColRef = functionDesc.getColRefs().get(1);
        functionDesc.getConfiguration().put(TopNMeasureType.CONFIG_ENCODING_PREFIX + sellerColRef.getIdentity(),
                "int:6");
        colsNeedDict = measureType.getColumnsNeedDictionary(functionDesc);

        assertTrue(colsNeedDict.size() == 0);

        measureType.validate(functionDesc);

    }

    private FunctionDesc initFunction() throws Exception {
        DataType returnDataType = new DataType(TopNMeasureType.DATATYPE_TOPN, 100, 0);

        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "SELLER_ID", "long");
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "TOTAL_PRICE", "double");

        FunctionDesc functionDesc = FunctionDesc.newInstance(TopNMeasureType.FUNC_TOP_N, //
                Lists.newArrayList(ParameterDesc.newInstance(col1), ParameterDesc.newInstance(col2)),
                returnDataType.toString());

        return functionDesc;
    }

    private void initField(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        Unsafe.changeAccessibleObject(field, true);
        field.set(obj, value);
    }

    public static class MockDictionary extends Dictionary<String> {
        private static final long serialVersionUID = 1L;

        public String[] values;

        @Override
        public int getMinId() {
            return 0;
        }

        @Override
        public int getMaxId() {
            return values.length - 1;
        }

        @Override
        public int getSizeOfId() {
            return 4;
        }

        @Override
        public int getSizeOfValue() {
            return 4;
        }

        @Override
        protected int getIdFromValueImpl(String value, int roundingFlag) {
            return 0;
        }

        @Override
        protected String getValueFromIdImpl(int id) {
            return "" + values[id];
        }

        @Override
        public void dump(PrintStream out) {
        }

        @Override
        public void write(DataOutput out) throws IOException {
        }

        @Override
        public void readFields(DataInput in) throws IOException {
        }

        @Override
        public boolean contains(Dictionary another) {
            return false;
        }
    }
}
