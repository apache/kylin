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

import static org.junit.Assert.*;

import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.cube.model.validation.rule.RowKeyAttrRule;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;

/**
 * @author jianliu
 * 
 */
public class RowKeyAttrRuleTest {

    private CubeDesc cube;
    private ValidateContext vContext = new ValidateContext();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        CubeDesc desc2 = JsonUtil.readValue(getClass().getClassLoader().getResourceAsStream("data/TEST3_desc.json"), CubeDesc.class);
        this.cube = desc2;

    }

    @Test
    public void testOneMandatoryColumn() {
        IValidatorRule<CubeDesc> rule = new RowKeyAttrRule();
        rule.validate(cube, vContext);
        vContext.print(System.out);
        assertTrue("Failed to validate rowkey", vContext.getResults().length == 1);
        assertTrue("Failed to validate mandatory error", vContext.getResults()[0].getMessage().startsWith("Rowkey column"));
    }
}
