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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Maps;

/**
 * @author yangli9
 */
public class CubeDescTest extends LocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGoodInit() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit1() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 includes field not set");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        String[] temp = null;
        cubeDesc.getAggregationGroups().get(0).setIncludes(temp);

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit2() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 select rule field not set");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        SelectRule temp = null;
        cubeDesc.getAggregationGroups().get(0).setSelectRule(temp);

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit3() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 Include dims not containing all the used dims");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        String[] temp = Arrays.asList(cubeDesc.getAggregationGroups().get(0).getIncludes()).subList(0, 3).toArray(new String[3]);
        cubeDesc.getAggregationGroups().get(0).setIncludes(temp);

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit4() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 has too many combinations, use 'mandatory'/'hierarchy'/'joint' to optimize; or update 'kylin.cube.aggrgroup.max.combination' to a bigger value.");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        try {
            System.setProperty("kylin.cube.aggrgroup.max.combination", "8");
            cubeDesc.validate();
        } finally {
            System.clearProperty("kylin.cube.aggrgroup.max.combination");
        }
    }

    @Test
    public void testBadInit5() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().mandatory_dims = new String[] { "seller_id", "META_CATEG_NAME" };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit6() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().mandatory_dims = new String[] { "seller_id", "lstg_format_name" };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit7() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 require at least 2 dims in a joint");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { "lstg_format_name" } };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit8() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 hierarchy dims overlap with joint dims");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { "META_CATEG_NAME", "CATEG_LVL2_NAME" } };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit9() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 hierarchy dims overlap with joint dims");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchy_dims = new String[][] { new String[] { "META_CATEG_NAME", "CATEG_LVL2_NAME", "CATEG_LVL3_NAME" }, new String[] { "lstg_format_name", "lstg_site_id" } };
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { "META_CATEG_NAME", "lstg_format_name" } };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit10() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 a dim exist in more than one joint");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { "lstg_format_name", "lstg_site_id", "slr_segment_cd" }, new String[] { "lstg_format_name", "lstg_site_id", "leaf_categ_id" } };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit11() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 require at least 2 dims in a hierarchy");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchy_dims = new String[][] { new String[] { "META_CATEG_NAME" } };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testBadInit12() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 a dim exist in more than one hierarchy");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchy_dims = new String[][] { new String[] { "META_CATEG_NAME", "CATEG_LVL2_NAME", "CATEG_LVL3_NAME" }, new String[] { "META_CATEG_NAME", "CATEG_LVL2_NAME" } };

        cubeDesc.init(getTestConfig(), MetadataManager.getInstance(getTestConfig()).getAllTablesMap());
    }

    @Test
    public void testSerialize() throws Exception {
        CubeDesc desc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        String str = JsonUtil.writeValueAsIndentString(desc);
        System.out.println(str);
        @SuppressWarnings("unused")
        CubeDesc desc2 = JsonUtil.readValue(str, CubeDesc.class);
    }

    @Test
    public void testGetCubeDesc() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        Assert.assertNotNull(cubeDesc);
    }

    @Test
    public void testSerializeMap() throws Exception {
        Map<String, String> map = Maps.newHashMap();

        map.put("key1", "value1");
        map.put("key2", "value2");

        String mapStr = JsonUtil.writeValueAsString(map);

        System.out.println(mapStr);

        Map<?, ?> map2 = JsonUtil.readValue(mapStr, HashMap.class);

        Assert.assertEquals(map, map2);

    }

}
