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

package org.apache.kylin.cube;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.cube.model.validation.rule.AggregationGroupSizeRule;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jianliu
 * 
 */
public class AggregationGroupSizeRuleTest {

    private CubeDesc cube;
    private ValidateContext vContext = new ValidateContext();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        CubeDesc desc2 = JsonUtil.readValue(getClass().getClassLoader().getResourceAsStream("data/TEST2_desc.json"), CubeDesc.class);
        this.cube = desc2;

    }

    @Test
    public void testOneMandatoryColumn() {
        IValidatorRule<CubeDesc> rule = new AggregationGroupSizeRule() {
            /*
             * (non-Javadoc)
             * 
             * @see
             * org.apache.kylin.metadata.validation.rule.AggregationGroupSizeRule
             * #getMaxAgrGroupSize()
             */
            @Override
            protected int getMaxAgrGroupSize() {
                return 3;
            }
        };
        rule.validate(cube, vContext);
        vContext.print(System.out);
        assertEquals("Failed to validate aggragation group error", vContext.getResults().length, 2);
        assertTrue("Failed to validate aggragation group error", vContext.getResults()[0].getMessage().startsWith("Length of the number"));
        assertTrue("Failed to validate aggragation group error", vContext.getResults()[1].getMessage().startsWith("Length of the number"));
        // assertTrue("Failed to validate aggragation group error",
        // vContext.getResults()[2].getMessage()
        // .startsWith("Hierachy column"));
    }

    @Test
    public void testAggColumnSize() {
        AggregationGroupSizeRule rule = new AggregationGroupSizeRule() {
            /*
             * (non-Javadoc)
             * 
             * @see
             * org.apache.kylin.metadata.validation.rule.AggregationGroupSizeRule
             * #getMaxAgrGroupSize()
             */
            @Override
            protected int getMaxAgrGroupSize() {
                return 20;
            }
        };
        rule.validate(cube, vContext);
        vContext.print(System.out);
        assertEquals("Failed to validate aggragation group error", vContext.getResults().length, 0);
        // assertTrue("Failed to validate aggragation group error",
        // vContext.getResults()[0].getMessage()
        // .startsWith("Aggregation group"));
        // assertTrue("Failed to validate aggragation group error",
        // vContext.getResults()[0].getMessage()
        // .startsWith("Hierachy column"));
    }
}
