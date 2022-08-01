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
package org.apache.kylin.metadata.model.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class FunctionDescTest extends NLocalFileMetadataTestCase {

    private static NDataModel model;
    private static final String DF_NAME = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
    private static final String PROJECT = "default";

    @Before
    public void setUp() {
        createTestMetadata();
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        model = modelMgr.getDataModelDesc(DF_NAME);
    }

    @Test
    public void testProposeReturnType() {
        Assert.assertEquals("bigint", FunctionDesc.proposeReturnType("COUNT", "int"));
        Assert.assertEquals("topn(100, 4)", FunctionDesc.proposeReturnType("TOP_N", "int"));
        Assert.assertEquals("bigint", FunctionDesc.proposeReturnType("SUM", "tinyint"));
        Assert.assertEquals("decimal(19,4)", FunctionDesc.proposeReturnType("SUM", null));
        //by default in KylinConfig,decimal is decimal(19,4)
        Assert.assertEquals("decimal(29,4)", FunctionDesc.proposeReturnType("SUM", "decimal"));
        Assert.assertEquals("decimal(29,2)", FunctionDesc.proposeReturnType("SUM", "decimal(19,2)"));
        //in sum(),scale and precision is up to 38
        Assert.assertEquals("decimal(38,2)", FunctionDesc.proposeReturnType("SUM", "decimal(29,2)"));
        Assert.assertEquals("decimal(38,6)", FunctionDesc.proposeReturnType("SUM", "decimal(39,6)"));
        Assert.assertEquals("decimal(38,38)", FunctionDesc.proposeReturnType("SUM", "decimal(40,39)"));
        Assert.assertEquals("hllc(10)",
                FunctionDesc.proposeReturnType("COUNT_DISTINCT", "int", new HashMap<String, String>() {
                    {
                        put("COUNT_DISTINCT", "hllc(10)");
                    }
                }));
    }

    @Test
    public void testInvalidMeasureColType() {
        try {
            FunctionDesc.proposeReturnType("SUM", "char", Maps.newHashMap(), true);
            Assert.fail();
        } catch (KylinException ignored) {
        }
        try {
            FunctionDesc.proposeReturnType("PERCENTILE_APPROX", "char", Maps.newHashMap(), true);
            Assert.fail();
        } catch (KylinException ignored) {
        }
    }

    @Test
    public void testRewriteFieldName() {
        FunctionDesc function = FunctionDesc.newInstance("count", newParameters(model.findColumn("TRANS_ID")),
                "bigint");
        Assert.assertEquals("_KY_COUNT_TEST_KYLIN_FACT_TRANS_ID_", function.getRewriteFieldName());

        FunctionDesc function1 = FunctionDesc.newInstance("count", newParameters("1"), "bigint");
        Assert.assertEquals("_KY_COUNT__", function1.getRewriteFieldName());
    }

    @Test
    public void testEquals() {
        FunctionDesc functionDesc = FunctionDesc.newInstance("COUNT", newParameters("1"), "bigint");
        FunctionDesc functionDesc1 = FunctionDesc.newInstance("COUNT", null, null);
        Assert.assertTrue(functionDesc.equals(functionDesc1));
        Assert.assertTrue(functionDesc1.equals(functionDesc));

        FunctionDesc functionDesc2 = FunctionDesc.newInstance("COUNT",
                newParameters(TblColRef.mockup(TableDesc.mockup("test"), 1, "name", null)), "bigint");
        Assert.assertFalse(functionDesc1.equals(functionDesc2));

        FunctionDesc functionDesc3 = FunctionDesc.newInstance("COUNT",
                newParameters(TblColRef.mockup(TableDesc.mockup("test"), 1, "name", null)), "bigint");
        Assert.assertTrue(functionDesc2.equals(functionDesc3));
    }

    @Test
    public void testRewriteFieldType() {
        FunctionDesc cnt1 = FunctionDesc.newInstance("COUNT", newParameters("1"), "bigint");
        Assert.assertEquals(DataType.getType("bigint"), cnt1.getReturnDataType());
        Assert.assertEquals(DataType.getType("bigint"), cnt1.getRewriteFieldType());

        FunctionDesc cnt2 = FunctionDesc.newInstance("COUNT", newParameters("1"), "integer");
        Assert.assertEquals(DataType.getType("integer"), cnt2.getReturnDataType());
        Assert.assertEquals(DataType.getType("bigint"), cnt2.getRewriteFieldType());

        FunctionDesc cnt3 = FunctionDesc.newInstance("COUNT", null, null);
        Assert.assertNull(cnt3.getReturnDataType());
        Assert.assertEquals(DataType.ANY, cnt3.getRewriteFieldType());

        FunctionDesc sum1 = FunctionDesc.newInstance("SUM", newParameters("1"), "bigint");
        Assert.assertEquals(DataType.getType("bigint"), sum1.getReturnDataType());
        Assert.assertEquals(DataType.getType("bigint"), sum1.getRewriteFieldType());

        FunctionDesc max = FunctionDesc.newInstance("MAX", newParameters(TblColRef.mockup(null, 0, "col", "integer")),
                "bigint");
        Assert.assertEquals(DataType.getType("bigint"), max.getReturnDataType());
        Assert.assertEquals(DataType.getType("integer"), max.getRewriteFieldType());
    }

    @Test
    public void testIsDatatypeSuitable() {
        List<DataType> allDataTypes = Stream
                .of("short", "long", "int4", "long8", "byte", "binary", "numeric", "datetime", "time", "real", "any",
                        "varchar", "char", "integer", "tinyint", "smallint", "bigint", "float", "decimal", "double",
                        "array", "timestamp", "date", "string", "boolean", "int")
                .map(DataType::getType).collect(Collectors.toList());

        List<String> allFunctions = Arrays.asList("SUM", "MIN", "MAX", "COUNT", "COLLECT_SET", "COUNT_DISTINCT",
                "PERCENTILE_APPROX", "TOP_N");

        Map<String, List<DataType>> funcSuitableDataTypeMap = new HashMap<>();
        funcSuitableDataTypeMap.put("SUM",
                Stream.of("tinyint", "smallint", "integer", "bigint", "float", "double", "decimal")
                        .map(DataType::getType).collect(Collectors.toList()));
        funcSuitableDataTypeMap.put("TOP_N",
                Stream.of("tinyint", "smallint", "integer", "bigint", "float", "double", "decimal")
                        .map(DataType::getType).collect(Collectors.toList()));
        funcSuitableDataTypeMap.put("PERCENTILE_APPROX", Stream.of("tinyint", "smallint", "integer", "bigint")
                .map(DataType::getType).collect(Collectors.toList()));

        for (DataType dataType : allDataTypes) {
            for (String function : allFunctions) {
                FunctionDesc functionDesc = FunctionDesc.newInstance(function,
                        newParameters(TblColRef.mockup(null, 0, "col", dataType.getName())), null);
                Assert.assertEquals(functionDesc.isDatatypeSuitable(dataType),
                        funcSuitableDataTypeMap.getOrDefault(function, allDataTypes).contains(dataType));
            }
        }
    }

    private static List<ParameterDesc> newParameters(Object... objs) {
        List<ParameterDesc> params = new ArrayList<>();
        for (Object obj : objs) {
            params.add(ParameterDesc.newInstance(obj));
        }
        return params;
    }
}
