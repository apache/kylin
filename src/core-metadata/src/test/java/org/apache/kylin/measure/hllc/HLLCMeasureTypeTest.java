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

package org.apache.kylin.measure.hllc;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HLLCMeasureTypeTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testIngest() {
        MeasureType<HLLCounter> mtype = (MeasureType<HLLCounter>) MeasureTypeFactory
                .create(HLLCMeasureType.FUNC_COUNT_DISTINCT, DataType.getType("hllc(10)"));
        MeasureIngester<HLLCounter> ingester = mtype.newIngester();
        HLLCounter hllc;

        hllc = ingester.valueOf(new String[] { null }, null, null);
        assertEquals(0, hllc.getCountEstimate());

        hllc = ingester.valueOf(new String[] { null, null }, null, null);
        assertEquals(0, hllc.getCountEstimate());

        hllc = ingester.valueOf(new String[] { "" }, null, null);
        assertEquals(1, hllc.getCountEstimate());

        hllc = ingester.valueOf(new String[] { "", null }, null, null);
        assertEquals(1, hllc.getCountEstimate());

        hllc = ingester.valueOf(new String[] { "abc" }, null, null);
        assertEquals(1, hllc.getCountEstimate());

        FunctionDesc functionDesc = FunctionDesc.newInstance(HLLCMeasureType.FUNC_COUNT_DISTINCT, null, "hllc(10)");
        mtype.validate(functionDesc);
    }
}
