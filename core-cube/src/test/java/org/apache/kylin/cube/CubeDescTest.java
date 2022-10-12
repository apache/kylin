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

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.cube.model.CubeDesc.DeriveType;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.cube.model.TooManyCuboidException;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

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

    @BeforeEach
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
            if (c.toLowerCase(Locale.ROOT).contains(name.toLowerCase(Locale.ROOT)))
                return c;
        }
        throw new IllegalStateException();
    }

    @AfterEach
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    void testCiCube() {
        CubeDescManager mgr = CubeDescManager.getInstance(getTestConfig());
        CubeDesc lc = mgr.getCubeDesc("ci_left_join_cube");
        CubeDesc ic = mgr.getCubeDesc("ci_inner_join_cube");
        assertNotNull(lc);
        assertNotNull(ic);

        // assert the two CI cubes are identical apart from the left/inner difference
        assertEquals(lc.getDimensions().size(), ic.getDimensions().size());
        for (int i = 0, n = lc.getDimensions().size(); i < n; i++) {
            DimensionDesc ld = lc.getDimensions().get(i);
            DimensionDesc id = ic.getDimensions().get(i);
            assertEquals(ld.getTable(), id.getTable());
            assertEquals(ld.getColumn(), id.getColumn());
            assertArrayEquals(ld.getDerived(), id.getDerived());
        }

        // To enable spark in IT, the inner cube removed the percentile measure, so ignore that particular measure
        List<MeasureDesc> lcMeasures = dropPercentile(lc.getMeasures());
        List<MeasureDesc> icMeasures = ic.getMeasures();

        assertEquals(lcMeasures.size(), icMeasures.size());
        for (int i = 0, n = lcMeasures.size(); i < n; i++) {
            MeasureDesc lm = lcMeasures.get(i);
            MeasureDesc im = icMeasures.get(i);
            assertEquals(lm.getName(), im.getName());
            assertEquals(lm.getFunction().getFullExpression(), im.getFunction().getFullExpression());
            assertEquals(lm.getFunction().getReturnType(), im.getFunction().getReturnType());
        }

        assertEquals(lc.getAggregationGroups().size(), ic.getAggregationGroups().size());
        for (int i = 0, n = lc.getAggregationGroups().size(); i < n; i++) {
            AggregationGroup lag = lc.getAggregationGroups().get(i);
            AggregationGroup iag = ic.getAggregationGroups().get(i);
            assertArrayEquals(lag.getIncludes(), iag.getIncludes());
            assertArrayEquals(lag.getSelectRule().mandatoryDims, iag.getSelectRule().mandatoryDims);
            assertArrayEquals(lag.getSelectRule().hierarchyDims, iag.getSelectRule().hierarchyDims);
            assertArrayEquals(lag.getSelectRule().jointDims, iag.getSelectRule().jointDims);
        }

        assertEquals(lc.listAllColumnDescs().size(), ic.listAllColumnDescs().size());
        assertEquals(lc.listAllColumns().size(), ic.listAllColumns().size());

        // test KYLIN-2440
        assertTrue(lc.listAllColumns().contains(lc.getModel().findColumn("SELLER_ACCOUNT.ACCOUNT_ID")));
        assertTrue(ic.listAllColumns().contains(ic.getModel().findColumn("SELLER_ACCOUNT.ACCOUNT_ID")));
    }

    private List<MeasureDesc> dropPercentile(List<MeasureDesc> measures) {
        ArrayList<MeasureDesc> result = new ArrayList<>();
        for (MeasureDesc m : measures) {
            if (!m.getFunction().getExpression().toUpperCase(Locale.ROOT).contains("PERCENTILE"))
                result.add(m);
        }
        return result;
    }

    @Test
    void testGoodInit() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.init(getTestConfig());
    }

    @Test
    void testBadInit1() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        String[] temp = null;

        cubeDesc.getAggregationGroups().get(0).setIncludes(temp);
        Assertions.assertEquals(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> cubeDesc.init(getTestConfig())
            ).getMessage(),
            "AggregationGroup incomplete"
        );
    }

    @Test
    void testBadInit2() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        SelectRule temp = null;
        cubeDesc.getAggregationGroups().get(0).setSelectRule(temp);
        Assertions.assertEquals(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> cubeDesc.init(getTestConfig())
            ).getMessage(),
            "AggregationGroup incomplete"
        );
    }

    @Test
    void testBadInit3() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        String[] temp = Arrays.asList(cubeDesc.getAggregationGroups().get(0).getIncludes()).subList(0, 3)
            .toArray(new String[3]);
        cubeDesc.getAggregationGroups().get(0).setIncludes(temp);

        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> cubeDesc.init(getTestConfig())
            ).getMessage(),
            CoreMatchers.startsWith(
                "Aggregation group 1 'includes' dimensions not include all the dimensions:"
            )
        );
    }

    @Test
    void testBadInit4() {
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                TooManyCuboidException.class,
                () -> {
                    CubeDesc cubeDesc = CubeDescManager
                        .getInstance(getTestConfig())
                        .getCubeDesc(CUBE_WITH_SLR_DESC);
                    try {
                        System.setProperty("kylin.cube.aggrgroup.max-combination", "8");
                        cubeDesc.validateAggregationGroups();
                        cubeDesc.validateAggregationGroupsCombination();
                    } finally {
                        System.clearProperty("kylin.cube.aggrgroup.max-combination");
                    }
                }
            ).getMessage(),
            CoreMatchers.is(
                "Aggregation group 1 of Cube Desc test_kylin_cube_with_slr_desc has too many combinations: 31. Use 'mandatory'/'hierarchy'/'joint' to optimize; or update 'kylin.cube.aggrgroup.max-combination' to a bigger value."
            )
        );
    }

    @Test
    void testBadInit5() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().mandatoryDims = new String[]{SELLER_ID,
            META_CATEG_NAME};

        cubeDesc.init(getTestConfig());
    }

    @Test
    void testBadInit6() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.getAggregationGroups().get(0).getSelectRule().mandatoryDims = new String[]{SELLER_ID,
            LSTG_FORMAT_NAME};

        cubeDesc.init(getTestConfig());
    }

    @Test
    void testBadInit7() {
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
                    cubeDesc.getAggregationGroups().get(0).getSelectRule().jointDims = new String[][]{
                        new String[]{LSTG_FORMAT_NAME}};

                    cubeDesc.init(getTestConfig());
                }
            ).getMessage(),
            CoreMatchers.startsWith("Aggregation group 1 require at least 2 dimensions in a joint")
        );
    }

    @Test
    void testBadInit8() {
        String[] strs = new String[]{CATEG_LVL2_NAME, META_CATEG_NAME};
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
                    cubeDesc.getAggregationGroups().get(0).getSelectRule().jointDims = new String[][]{
                        new String[]{META_CATEG_NAME, CATEG_LVL2_NAME}};

                    cubeDesc.init(getTestConfig());
                }
            ).getMessage(),
            CoreMatchers.startsWith("Aggregation group 1 hierarchy dimensions overlap with joint dimensions: " + sortStrs(strs))
        );
    }

    @Test
    void testBadInit9() {
        String[] strs = new String[]{LSTG_FORMAT_NAME, META_CATEG_NAME};
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
                    cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchyDims = new String[][]{
                        new String[]{META_CATEG_NAME, CATEG_LVL2_NAME, CATEG_LVL3_NAME},
                        new String[]{LSTG_FORMAT_NAME, LSTG_SITE_ID}};
                    cubeDesc.getAggregationGroups().get(0).getSelectRule().jointDims = new String[][]{
                        new String[]{META_CATEG_NAME, LSTG_FORMAT_NAME}};

                    cubeDesc.init(getTestConfig());
                }
            ).getMessage(),
            CoreMatchers.startsWith("Aggregation group 1 hierarchy dimensions overlap with joint dimensions: " + sortStrs(strs))
        );
    }

    @Test
    void testBadInit10() {
        String[] strs = new String[]{LSTG_FORMAT_NAME, LSTG_SITE_ID};
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
                    cubeDesc.getAggregationGroups().get(0).getSelectRule().jointDims = new String[][]{
                        new String[]{LSTG_FORMAT_NAME, LSTG_SITE_ID, SLR_SEGMENT_CD},
                        new String[]{LSTG_FORMAT_NAME, LSTG_SITE_ID, LEAF_CATEG_ID}};

                    cubeDesc.init(getTestConfig());
                }
            ).getMessage(),
            CoreMatchers.startsWith("Aggregation group 1 a dimension exist in more than one joint: " + sortStrs(strs))
        );

    }

    @Test
    void testBadInit11() {
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
                    cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchyDims = new String[][]{
                        new String[]{META_CATEG_NAME}};

                    cubeDesc.init(getTestConfig());
                }
            ).getMessage(),
            CoreMatchers.startsWith("Aggregation group 1 require at least 2 dimensions in a hierarchy.")
        );

    }

    @Test
    void testBadInit12() {
        String[] strs = new String[]{CATEG_LVL2_NAME, META_CATEG_NAME};
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
                    cubeDesc.getAggregationGroups().get(0).getSelectRule().hierarchyDims = new String[][]{
                        new String[]{META_CATEG_NAME, CATEG_LVL2_NAME, CATEG_LVL3_NAME},
                        new String[]{META_CATEG_NAME, CATEG_LVL2_NAME}};

                    cubeDesc.init(getTestConfig());
                }
            ).getMessage(),
            CoreMatchers.startsWith("Aggregation group 1 a dimension exist in more than one hierarchy: " + sortStrs(strs))
        );

    }

    @Test
    void testBadInit14() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        MeasureDesc measureForTransCnt = cubeDesc.getMeasures().get(3);
        Assertions.assertEquals(measureForTransCnt.getName(), "TRANS_CNT");
        String measureInfoForTransCnt = measureForTransCnt.toString();
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    HBaseColumnDesc colDesc = new HBaseColumnDesc();
                    colDesc.setQualifier("M");
                    colDesc.setMeasureRefs(new String[]{"GMV_SUM", "GMV_MIN", "GMV_MAX", "ITEM_COUNT_SUM"});
                    cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0] = colDesc;
                    cubeDesc.initMeasureReferenceToColumnFamily();
                }
            ).getMessage(),
            CoreMatchers.startsWith("measure (" + measureInfoForTransCnt + ") does not exist in column family, or measure duplicates")
        );
    }

    @Test
    void testBadInit15() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        MeasureDesc measureForTransCnt = cubeDesc.getMeasures().get(3);
        Assertions.assertEquals(measureForTransCnt.getName(), "TRANS_CNT");
        HBaseColumnDesc colDesc = new HBaseColumnDesc();
        MatcherAssert.assertThat(
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    colDesc.setQualifier("M");
                    colDesc.setMeasureRefs(
                        new String[]{"GMV_SUM", "GMV_MIN", "GMV_MAX", "TRANS_CNT", "TRANS_CNT", "ITEM_COUNT_SUM"});
                    cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0] = colDesc;
                    cubeDesc.initMeasureReferenceToColumnFamily();

                }
            ).getMessage(),
            CoreMatchers.startsWith(
                "measure (" + measureForTransCnt.getName() + ") duplicates"
            )
        );
    }

    @Test
    void testCombinationIntOverflow() {
        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA, "cube_desc").listFiles()) {
            if (f.getName().endsWith(".bad")) {
                String path = f.getPath();
                f.renameTo(new File(path.substring(0, path.length() - 4)));
            }
        }
        Assertions.assertThrows(
            TooManyCuboidException.class,
            () -> {
                getTestConfig().clearManagers();
                CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig())
                    .getCubeDesc("ut_cube_desc_combination_int_overflow");
                cubeDesc.init(getTestConfig());
            }
        );
    }

    @Test
    void testTooManyRowkeys() {
        File metaFile = new File(LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA, "cube_desc/ut_78_rowkeys.json.bad");
        Assertions.assertTrue(metaFile.exists());
        String path = metaFile.getPath();
        metaFile.renameTo(new File(path.substring(0, path.length() - 4)));
        getTestConfig().clearManagers();
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("ut_78_rowkeys");
        Assertions.assertEquals(
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> cubeDesc.init(getTestConfig())
            ).getMessage(),
            "Too many rowkeys (78) in CubeDesc, please try to reduce dimension number or adopt derived dimensions"
        );
    }

    @Test
    void testValidateNotifyList() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        List<String> notify = Lists.newArrayList();
        notify.add("test");
        cubeDesc.setNotifyList(notify);
        Assertions.assertEquals(
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> cubeDesc.validateNotifyList()
            ).getMessage(),
            "Email [test] is not validation."
        );

    }

    @Test
    void testSerialize() throws Exception {
        CubeDesc desc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        String str = JsonUtil.writeValueAsIndentString(desc);
        //System.out.println(str);
        @SuppressWarnings("unused")
        CubeDesc desc2 = JsonUtil.readValue(str, CubeDesc.class);
    }

    @Test
    void testGetCopyOf() throws Exception {
        CubeDesc desc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        CubeDesc copyDesc = CubeDesc.getCopyOf(desc);

        // uuid is different, set to equals for json comparison
        copyDesc.setUuid(desc.getUuid());
        copyDesc.setLastModified(desc.getLastModified());

        String descStr = JsonUtil.writeValueAsIndentString(desc);
        String copyStr = JsonUtil.writeValueAsIndentString(copyDesc);

        assertEquals(descStr, copyStr);
    }

    @Test
    void testGetCubeDesc() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        Assertions.assertNotNull(cubeDesc);
    }

    @Test
    void testSerializeMap() throws Exception {
        Map<String, String> map = Maps.newHashMap();

        map.put("key1", "value1");
        map.put("key2", "value2");

        String mapStr = JsonUtil.writeValueAsString(map);

        //System.out.println(mapStr);

        Map<?, ?> map2 = JsonUtil.readValue(mapStr, HashMap.class);

        Assertions.assertEquals(map, map2);
    }

    @Test
    void testDerivedInfo() {
        {
            CubeDesc cube = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
            List<TblColRef> givenCols = new ArrayList<>();
            givenCols.add(cube.findColumnRef("TEST_KYLIN_FACT", "LSTG_SITE_ID"));
            givenCols.add(cube.findColumnRef("TEST_KYLIN_FACT", "LEAF_CATEG_ID"));
            Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = cube.getHostToDerivedInfo(givenCols, null);
            assertEquals(3, hostToDerivedInfo.size());
            assertEquals(Pair.newPair(3, 2), countDerivedInfo(hostToDerivedInfo));
        }

        {
            CubeDesc cube = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("ssb");
            List<TblColRef> givenCols = new ArrayList<>();
            givenCols.add(cube.findColumnRef("V_LINEORDER", "LO_PARTKEY"));
            Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = cube.getHostToDerivedInfo(givenCols, null);
            assertEquals(1, hostToDerivedInfo.size());
            assertEquals(Pair.newPair(1, 1), countDerivedInfo(hostToDerivedInfo));
        }
    }

    private Pair<Integer, Integer> countDerivedInfo(Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo) {
        int pkfkCount = 0;
        int lookupCount = 0;
        for (Entry<Array<TblColRef>, List<DeriveInfo>> entry : hostToDerivedInfo.entrySet()) {
            for (DeriveInfo deriveInfo : entry.getValue()) {
                if (deriveInfo.type == DeriveType.PK_FK)
                    pkfkCount++;
                if (deriveInfo.type == DeriveType.LOOKUP)
                    lookupCount++;
            }
        }
        return Pair.newPair(pkfkCount, lookupCount);
    }

    private Collection<String> sortStrs(String[] strs) {
        Set<String> set = new TreeSet<>();
        for (String str : strs)
            set.add(str);
        return set;
    }

    @Test
    void testInitPartialCube() {
        CubeDescManager mgr = CubeDescManager.getInstance(getTestConfig());
        CubeDesc lc = mgr.getCubeDesc("ut_inner_join_cube_partial");

        Assertions.assertNotNull(lc);
        Assertions.assertTrue(lc.getAllCuboids().size() > 0);
    }

}
