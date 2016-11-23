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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.cube.model.validation.rule.RowKeyAttrRule;
import org.junit.Test;

public class RowKeyAttrRuleTest {

    @Test
    public void testGoodDesc() throws IOException {
        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/").listFiles()) {
            CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);
            ValidateContext vContext = new ValidateContext();
            IValidatorRule<CubeDesc> rule = new RowKeyAttrRule();
            rule.validate(desc, vContext);
            vContext.print(System.out);
            assertTrue(vContext.getResults().length == 0);
        }
    }

    @Test
    public void testBadDesc() throws IOException {
        ValidateContext vContext = new ValidateContext();
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/test_kylin_cube_with_slr_desc.json"), CubeDesc.class);
        desc.getRowkey().getRowKeyColumns()[2].setColumn("");
        IValidatorRule<CubeDesc> rule = new RowKeyAttrRule();
        rule.validate(desc, vContext);
        vContext.print(System.out);
        assertTrue(vContext.getResults().length == 1);
        assertTrue("Rowkey column empty".equalsIgnoreCase(vContext.getResults()[0].getMessage()));
    }
}
