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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SelectRule;
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

    private static final String CUBE_WITH_SLR_DESC = "test_kylin_cube_with_slr_desc";
    
    private String SELLER_ID;
    private String SLR_SEGMENT_CD;
    private String LSTG_FORMAT_NAME;
    private String LSTG_SITE_ID;
    private String META_CATEG_NAME;
    private String CATEG_LVL2_NAME;
    private String CATEG_LVL3_NAME;
    private String LEAF_CATEG_ID;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        AggregationGroup g = cubeDesc.getAggregationGroups().get(0);
        SELLER_ID = getColInAggrGroup(g, "SELLER_ID");
        SLR_SEGMENT_CD = getColInAggrGroup(g, "SLR_SEGMENT_CD");
        LSTG_FORMAT_NAME = getColInAggrGroup(g, "LSTG_FORMAT_NAME");
        LSTG_SITE_ID = getColInAggrGroup(g, "LSTG_SITE_ID");
        META_CATEG_NAME = getColInAggrGroup(g, "META_CATEG_NAME");
        CATEG_LVL2_NAME = getColInAggrGroup(g, "CATEG_LVL2_NAME");
        CATEG_LVL3_NAME = getColInAggrGroup(g, "CATEG_LVL3_NAME");
        LEAF_CATEG_ID = getColInAggrGroup(g, "LEAF_CATEG_ID");
    }

    private String getColInAggrGroup(AggregationGroup g, String name) {
        for (String c : g.getIncludes()) {
            if (c.toLowerCase().contains(name.toLowerCase()))
                return c;
        }
        throw new IllegalStateException();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGoodInit() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit1() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 includes field not set");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        String[] temp = null;
        cubeDesc.getAggregationGroups().get(0).setIncludes(temp);

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit2() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 select rule field not set");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        SelectRule temp = null;
        cubeDesc.getAggregationGroups().get(0).setSelectRule(temp);

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit3() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 'includes' dimensions not include all the dimensions:");
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        String[] temp = Arrays.asList(cubeDesc.getAggregationGroups().get(0).getIncludes()).subList(0, 3).toArray(new String[3]);
        cubeDesc.getAggregationGroups().get(0).setIncludes(temp);

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit4() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 has too many combinations, use 'mandatory'/'hierarchy'/'joint' to optimize; or update 'kylin.cube.aggrgroup.max-combination' to a bigger value.");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        try {
            System.setProperty("kylin.cube.aggrgroup.max-combination", "8");
            cubeDesc.validateAggregationGroups();
        } finally {
            System.clearProperty("kylin.cube.aggrgroup.max-combination");
        }
    }

    @Test
    public void testBadInit5() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().mandatory_dims = new String[] { SELLER_ID, META_CATEG_NAME };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit6() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().mandatory_dims = new String[] { SELLER_ID, LSTG_FORMAT_NAME };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit7() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 require at least 2 dimensions in a joint");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { LSTG_FORMAT_NAME } };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit8() throws Exception {
        String[] strs = new String[] { CATEG_LVL2_NAME, META_CATEG_NAME };
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 hierarchy dimensions overlap with joint dimensions: " + sortStrs(strs));

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { META_CATEG_NAME, CATEG_LVL2_NAME } };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit9() throws Exception {
        String[] strs = new String[] { LSTG_FORMAT_NAME, META_CATEG_NAME };
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 hierarchy dimensions overlap with joint dimensions: " + sortStrs(strs));
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchy_dims = new String[][] { new String[] { META_CATEG_NAME, CATEG_LVL2_NAME, CATEG_LVL3_NAME }, new String[] { LSTG_FORMAT_NAME, LSTG_SITE_ID } };
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { META_CATEG_NAME, LSTG_FORMAT_NAME } };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit10() throws Exception {
        String[] strs = new String[] { LSTG_FORMAT_NAME, LSTG_SITE_ID };
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 a dimension exist in more than one joint: " + sortStrs(strs));

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().joint_dims = new String[][] { new String[] { LSTG_FORMAT_NAME, LSTG_SITE_ID, SLR_SEGMENT_CD }, new String[] { LSTG_FORMAT_NAME, LSTG_SITE_ID, LEAF_CATEG_ID } };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit11() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 require at least 2 dimensions in a hierarchy.");

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchy_dims = new String[][] { new String[] { META_CATEG_NAME } };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testBadInit12() throws Exception {
        String[] strs = new String[] { CATEG_LVL2_NAME, META_CATEG_NAME };
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Aggregation group 0 a dimension exist in more than one hierarchy: " + sortStrs(strs));

        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchy_dims = new String[][] { new String[] { META_CATEG_NAME, CATEG_LVL2_NAME, CATEG_LVL3_NAME }, new String[] { META_CATEG_NAME, CATEG_LVL2_NAME } };

        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testCombinationIntOverflow() throws  Exception {
        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA, "cube_desc").listFiles()) {
            if (f.getName().endsWith(".bad")) {
                String path = f.getPath();
                f.renameTo(new File(path.substring(0, path.length() - 4)));
            }
        }

        thrown.expect(IllegalStateException.class);
        CubeDescManager.clearCache();
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("ut_cube_desc_combination_int_overflow");
        cubeDesc.init(getTestConfig());
    }

    @Test
    public void testSerialize() throws Exception {
        CubeDesc desc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        String str = JsonUtil.writeValueAsIndentString(desc);
        System.out.println(str);
        @SuppressWarnings("unused")
        CubeDesc desc2 = JsonUtil.readValue(str, CubeDesc.class);
    }

    @Test
    public void testGetCubeDesc() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
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

    private Collection<String> sortStrs(String[] strs) {
        Set<String> set = new TreeSet<>();
        for (String str : strs)
            set.add(str);
        return set;
    }

}
