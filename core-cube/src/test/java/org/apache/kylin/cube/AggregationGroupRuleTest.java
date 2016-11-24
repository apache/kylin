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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.cube.model.validation.rule.AggregationGroupRule;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AggregationGroupRuleTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGoodDesc() throws IOException {
        AggregationGroupRule rule = getAggregationGroupRule();

        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA + "/cube_desc/").listFiles()) {
            if (!f.getName().endsWith("json")) {
                continue;
            }
            CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);
            ValidateContext vContext = new ValidateContext();
            rule.validate(desc, vContext);
            vContext.print(System.out);
            assertTrue(vContext.getResults().length == 0);
        }
    }

    @Test
    public void testGoodBecomeBadDesc() throws IOException {
        AggregationGroupRule rule = new AggregationGroupRule() {
            @Override
            protected int getMaxCombinations(CubeDesc cubeDesc) {
                return 2;
            }
        };

        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA + "/cube_desc/").listFiles()) {
            System.out.println(f.getName());
            CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);
            ValidateContext vContext = new ValidateContext();
            rule.validate(desc, vContext);
            vContext.print(System.out);
            assertTrue(vContext.getResults().length > 0);
            assertTrue(vContext.getResults()[0].getMessage().startsWith("Aggregation group 0 has too many combinations"));
        }
    }

    @Test
    public void testGoodDesc2() throws IOException {

        ValidateContext vContext = new ValidateContext();
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA + "/cube_desc/test_kylin_cube_with_slr_desc.json"), CubeDesc.class);
        desc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { //
                new String[] { "lstg_format_name", "lstg_site_id", "slr_segment_cd", "CATEG_LVL2_NAME" } };

        IValidatorRule<CubeDesc> rule = getAggregationGroupRule();
        rule.validate(desc, vContext);
        vContext.print(System.out);
        assertEquals(0, vContext.getResults().length);
    }

    @Test
    public void testBadDesc1() throws IOException {

        ValidateContext vContext = new ValidateContext();
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA + "/cube_desc/test_kylin_cube_with_slr_desc.json"), CubeDesc.class);
        String[] temp = Arrays.asList(desc.getAggregationGroups().get(0).getIncludes()).subList(0, 3).toArray(new String[3]);

        desc.getAggregationGroups().get(0).setIncludes(temp);
        IValidatorRule<CubeDesc> rule = getAggregationGroupRule();
        rule.validate(desc, vContext);
        vContext.print(System.out);
        assertEquals(1, vContext.getResults().length);
        assertEquals("Aggregation group 0 'includes' dimensions not include all the dimensions:[seller_id, META_CATEG_NAME, lstg_format_name, lstg_site_id, slr_segment_cd]", (vContext.getResults()[0].getMessage()));
    }

    @Test
    public void testBadDesc2() throws IOException {

        ValidateContext vContext = new ValidateContext();
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA + "/cube_desc/test_kylin_cube_with_slr_desc.json"), CubeDesc.class);
        desc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { //
                new String[] { "lstg_format_name", "lstg_site_id", "slr_segment_cd", "META_CATEG_NAME", "CATEG_LVL2_NAME" } };

        IValidatorRule<CubeDesc> rule = getAggregationGroupRule();
        rule.validate(desc, vContext);
        vContext.print(System.out);
        assertEquals(1, vContext.getResults().length);
        assertEquals("Aggregation group 0 joint dimensions has overlap with more than 1 dimensions in same hierarchy: [CATEG_LVL2_NAME, META_CATEG_NAME]", (vContext.getResults()[0].getMessage()));
    }

    @Test
    public void testCombinationIntOverflow() throws IOException {
        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA + "/cube_desc/").listFiles()) {
            if (f.getName().endsWith("bad")) {
                String path = f.getPath();
                f.renameTo(new File(path.substring(0, path.length() - 4)));
            }
        }

        ValidateContext vContext = new ValidateContext();
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA + "/cube_desc/ut_cube_desc_combination_int_overflow.json"), CubeDesc.class);

        IValidatorRule<CubeDesc> rule = getAggregationGroupRule();
        rule.validate(desc, vContext);
        assertEquals(1, vContext.getResults().length);
    }

    public AggregationGroupRule getAggregationGroupRule() {
        AggregationGroupRule rule = new AggregationGroupRule() {
            @Override
            protected int getMaxCombinations(CubeDesc cubeDesc) {
                return 4096;
            }
        };

        return rule;
    }
}
