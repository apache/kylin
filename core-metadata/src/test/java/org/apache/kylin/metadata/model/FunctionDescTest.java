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
package org.apache.kylin.metadata.model;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FunctionDescTest {

    @Before
    public void setUp() throws Exception {
        System.setProperty(KylinConfig.KYLIN_CONF, LocalFileMetadataTestCase.LOCALMETA_TEST_DATA);
    }

    @Test
    public void getRewriteFieldType() {
        TblColRef mockColOfDoubleType = TblColRef.mockup(TableDesc.mockup("mock_table"), 0, "price", "double", "");
        TblColRef mockColOfDecimalType = TblColRef.mockup(TableDesc.mockup("mock_table"), 1, "price", "decimal", "");
        TblColRef mockColOfIntegerType = TblColRef.mockup(TableDesc.mockup("mock_table"), 2, "price", "integer", "");

        FunctionDesc function = FunctionDesc.newInstance("SUM", ParameterDesc.newInstance("1"), "bigint");
        assertEquals(DataType.getType("bigint"), function.getRewriteFieldType());
        function = FunctionDesc.newInstance("COUNT", ParameterDesc.newInstance("1"), "bigint");
        assertEquals(DataType.getType("bigint"), function.getRewriteFieldType());
        function = FunctionDesc.newInstance("SUM", ParameterDesc.newInstance(mockColOfDoubleType), "double");
        assertEquals(DataType.getType("double"), function.getRewriteFieldType());
        function = FunctionDesc.newInstance("SUM", ParameterDesc.newInstance(mockColOfIntegerType), "bigint");
        assertEquals(DataType.getType("bigint"), function.getRewriteFieldType());
        function = FunctionDesc.newInstance("MAX", ParameterDesc.newInstance(mockColOfDecimalType), "double");
        assertEquals(DataType.getType("decimal"), function.getRewriteFieldType());
        function = FunctionDesc.newInstance("MIN", ParameterDesc.newInstance(mockColOfDecimalType), "double");
        assertEquals(DataType.getType("decimal"), function.getRewriteFieldType());
        function = FunctionDesc.newInstance(FunctionDesc.FUNC_PERCENTILE,
                ParameterDesc.newInstance(mockColOfDecimalType), "double");
        assertEquals(DataType.ANY, function.getRewriteFieldType());
    }

    @After
    public void tearDown() throws Exception {
        System.clearProperty(KylinConfig.KYLIN_CONF);
    }
}