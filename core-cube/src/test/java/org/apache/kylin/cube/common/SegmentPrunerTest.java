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

package org.apache.kylin.cube.common;

import static org.apache.kylin.metadata.filter.TupleFilter.and;
import static org.apache.kylin.metadata.filter.TupleFilter.compare;
import static org.apache.kylin.metadata.filter.TupleFilter.or;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.SetAndUnsetSystemProp;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.metadata.filter.DynamicTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

import java.util.Map;

public class SegmentPrunerTest extends LocalFileMetadataTestCase {
    private CubeInstance cube;

    @Before
    public void setUp() {
        this.createTestMetadata();
        cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("ssb_cube_with_dimention_range");
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testEmptySegment() {
        CubeSegment seg = cube.getFirstSegment();
        TblColRef col = cube.getModel().findColumn("CUSTOMER.C_NATION");

        // a normal hit
        TupleFilter f = compare(col, FilterOperatorEnum.EQ, "CHINA");
        SegmentPruner segmentPruner = new SegmentPruner(f);
        Assert.assertTrue(segmentPruner.check(seg));

        // make the segment empty, it should be pruned
        seg.setInputRecords(0);
        Assert.assertFalse(segmentPruner.check(seg));
    }

    @Test
    public void testDynamicFilter() {
        CubeSegment seg = cube.getFirstSegment();
        TblColRef col = cube.getModel().findColumn("CUSTOMER.C_NATION");

        // pass case of a dynamic filter
        {
            DynamicTupleFilter dyna = new DynamicTupleFilter("$0");
            CompareTupleFilter f = compare(col, FilterOperatorEnum.EQ, dyna);
            f.bindVariable("$0", "CHINA");
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(seg));
        }

        // prune case of a dynamic filter
        {
            DynamicTupleFilter dyna = new DynamicTupleFilter("$0");
            CompareTupleFilter f = compare(col, FilterOperatorEnum.EQ, dyna);
            f.bindVariable("$0", "XXXX");
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(seg) == false);
        }
    }

    @Test
    public void testDimensionRangeCheck() {
        CubeSegment cubeSegment = cube.getSegments().getFirstSegment();

        //integer
        TblColRef qtyCol = cube.getModel().findColumn("V_LINEORDER.LO_QUANTITY");
        TblColRef revCol = cube.getModel().findColumn("V_LINEORDER.V_REVENUE");
        
        TupleFilter constFilter_LO_QUANTITY0 = new ConstantTupleFilter(Sets.newHashSet("8", "18", "28"));//between min and max value
        TupleFilter constFilter_LO_QUANTITY1 = new ConstantTupleFilter("1");//min value
        TupleFilter constFilter_LO_QUANTITY2 = new ConstantTupleFilter("50");//max value
        TupleFilter constFilter_LO_QUANTITY3 = new ConstantTupleFilter("0");//lt min value
        TupleFilter constFilter_LO_QUANTITY4 = new ConstantTupleFilter("200");//gt max value
        TupleFilter constFilter_LO_QUANTITY5 = new ConstantTupleFilter(Sets.newHashSet("51", "52", "53"));//gt max values

        // non-constant filter
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.EQ, revCol);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(cubeSegment));
        }
        
        // is null
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.ISNULL);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(cubeSegment));
        }

        //lt min value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.LT, constFilter_LO_QUANTITY1);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertFalse(segmentPruner.check(cubeSegment));
        }

        //lte min value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.LTE, constFilter_LO_QUANTITY1);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(cubeSegment));
        }

        //lt max value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.LT, constFilter_LO_QUANTITY2);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(cubeSegment));
        }

        //gt max value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.GT, constFilter_LO_QUANTITY2);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertFalse(segmentPruner.check(cubeSegment));
        }

        //gte max value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.GTE, constFilter_LO_QUANTITY2);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(cubeSegment));
        }

        //gt min value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.GT, constFilter_LO_QUANTITY1);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(cubeSegment));
        }

        //in over-max values
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.IN, constFilter_LO_QUANTITY5);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertFalse(segmentPruner.check(cubeSegment));
        }

        //in normal values
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.IN, constFilter_LO_QUANTITY0);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertTrue(segmentPruner.check(cubeSegment));
        }

        //lte under-min value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.LTE, constFilter_LO_QUANTITY3);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertFalse(segmentPruner.check(cubeSegment));
        }

        //gte over-max value
        {
            TupleFilter f = compare(qtyCol, FilterOperatorEnum.GTE, constFilter_LO_QUANTITY4);
            SegmentPruner segmentPruner = new SegmentPruner(f);
            Assert.assertFalse(segmentPruner.check(cubeSegment));
        }
    }

    @Test
    public void testLegacyCubeSeg() {
        // legacy cube segments does not have DimensionRangeInfo, but with TSRange can do some pruning
        CubeInstance cube = CubeManager.getInstance(getTestConfig())
                .getCube("test_kylin_cube_without_slr_left_join_ready_2_segments");

        TblColRef col = cube.getModel().findColumn("TEST_KYLIN_FACT.CAL_DT");
        CubeSegment seg = cube.getSegments(SegmentStatusEnum.READY).get(0);
        TSRange tsRange = seg.getTSRange();
        long start = tsRange.start.v;

        try (SetAndUnsetSystemProp sns = new SetAndUnsetSystemProp("kylin.query.skip-empty-segments", "false")) {
            {
                TupleFilter f = compare(col, FilterOperatorEnum.LTE, start);
                SegmentPruner segmentPruner = new SegmentPruner(f);
                Assert.assertTrue(segmentPruner.check(seg));
            }
            {
                TupleFilter f = compare(col, FilterOperatorEnum.LT, start);
                SegmentPruner segmentPruner = new SegmentPruner(f);
                Assert.assertFalse(segmentPruner.check(seg));
            }
        }
    }

    @Test
    public void testLegacyCubeSegWithOrFilter() {
        // legacy cube segments does not have DimensionRangeInfo, but with TSRange can do some pruning
        CubeInstance cube = CubeManager.getInstance(getTestConfig())
                .getCube("test_kylin_cube_without_slr_left_join_ready_2_segments");

        TblColRef col = cube.getModel().findColumn("TEST_KYLIN_FACT.CAL_DT");
        TblColRef col2 = cube.getModel().findColumn("TEST_KYLIN_FACT.LSTG_SITE_ID");
        CubeSegment seg = cube.getSegments(SegmentStatusEnum.READY).get(0);
        Map<String, DimensionRangeInfo> dimensionRangeInfoMap = Maps.newHashMap();
        dimensionRangeInfoMap.put("TEST_KYLIN_FACT.LSTG_SITE_ID", new DimensionRangeInfo("10", "20"));
        seg.setDimensionRangeInfoMap(dimensionRangeInfoMap);
        TSRange tsRange = seg.getTSRange();
        String start = DateFormat.formatToTimeStr(tsRange.start.v, "yyyy-MM-dd");

        CubeSegment seg2 = cube.getSegments(SegmentStatusEnum.READY).get(1);
        Map<String, DimensionRangeInfo> dimensionRangeInfoMap2 = Maps.newHashMap();
        dimensionRangeInfoMap2.put("TEST_KYLIN_FACT.LSTG_SITE_ID", new DimensionRangeInfo("20", "30"));
        seg2.setDimensionRangeInfoMap(dimensionRangeInfoMap2);
        TSRange tsRange2 = seg2.getTSRange();
        String start2 = DateFormat.formatToTimeStr(tsRange2.start.v, "yyyy-MM-dd");

        try (SetAndUnsetSystemProp sns = new SetAndUnsetSystemProp("kylin.query.skip-empty-segments", "false")) {
            {
                TupleFilter andFilter1 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter2 = compare(col2, FilterOperatorEnum.EQ, "15");
                LogicalTupleFilter logicalAndFilter = and(andFilter1, andFilter2);

                TupleFilter andFilter3 = compare(col, FilterOperatorEnum.EQ, start2);
                TupleFilter andFilter4 = compare(col2, FilterOperatorEnum.EQ, "25");
                LogicalTupleFilter logicalAndFilter2 = and(andFilter3, andFilter4);

                LogicalTupleFilter finalFilter = or(logicalAndFilter, logicalAndFilter2);
                SegmentPruner segmentPruner = new SegmentPruner(finalFilter);
                Assert.assertTrue(segmentPruner.check(seg));
                Assert.assertTrue(segmentPruner.check(seg2));
            }

            {
                TupleFilter andFilter1 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter2 = compare(col2, FilterOperatorEnum.EQ, "15");
                LogicalTupleFilter logicalAndFilter = and(andFilter1, andFilter2);

                TupleFilter andFilter3 = compare(col, FilterOperatorEnum.EQ, start2);
                TupleFilter andFilter4 = compare(col2, FilterOperatorEnum.EQ, "35");
                LogicalTupleFilter logicalAndFilter2 = and(andFilter3, andFilter4);

                LogicalTupleFilter finalFilter = or(logicalAndFilter, logicalAndFilter2);
                SegmentPruner segmentPruner = new SegmentPruner(finalFilter);
                Assert.assertTrue(segmentPruner.check(seg));
                Assert.assertFalse(segmentPruner.check(seg2));
            }

            {
                TupleFilter andFilter1 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter2 = compare(col2, FilterOperatorEnum.EQ, "15");
                LogicalTupleFilter logicalAndFilter = and(andFilter1, andFilter2);

                TupleFilter andFilter3 = compare(col, FilterOperatorEnum.EQ, start2);
                TupleFilter andFilter4 = compare(col2, FilterOperatorEnum.EQ, "35");
                LogicalTupleFilter logicalAndFilter2 = and(andFilter3, andFilter4);

                LogicalTupleFilter finalFilter = and(logicalAndFilter, logicalAndFilter2);
                SegmentPruner segmentPruner = new SegmentPruner(finalFilter);
                Assert.assertFalse(segmentPruner.check(seg));
                Assert.assertFalse(segmentPruner.check(seg2));
            }

            {
                TupleFilter andFilter1 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter2 = compare(col2, FilterOperatorEnum.EQ, "15");
                LogicalTupleFilter logicalAndFilter = and(andFilter1, andFilter2);

                TupleFilter andFilter3 = compare(col, FilterOperatorEnum.LT, start2);
                TupleFilter andFilter4 = compare(col2, FilterOperatorEnum.LT, "35");
                LogicalTupleFilter logicalAndFilter2 = and(andFilter3, andFilter4);

                LogicalTupleFilter finalFilter = and(logicalAndFilter, logicalAndFilter2);
                SegmentPruner segmentPruner = new SegmentPruner(finalFilter);
                Assert.assertTrue(segmentPruner.check(seg));
                Assert.assertFalse(segmentPruner.check(seg2));
            }

            {
                TupleFilter andFilter1 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter2 = compare(col2, FilterOperatorEnum.EQ, "35");
                LogicalTupleFilter logicalAndFilter = or(andFilter1, andFilter2);

                TupleFilter andFilter3 = compare(col, FilterOperatorEnum.EQ, start2);
                TupleFilter andFilter4 = compare(col2, FilterOperatorEnum.LT, "35");
                LogicalTupleFilter logicalAndFilter2 = or(andFilter3, andFilter4);

                LogicalTupleFilter finalFilter = or(logicalAndFilter, logicalAndFilter2);
                SegmentPruner segmentPruner = new SegmentPruner(finalFilter);
                Assert.assertTrue(segmentPruner.check(seg));
                Assert.assertTrue(segmentPruner.check(seg2));
            }

            {
                TupleFilter andFilter1 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter2 = compare(col2, FilterOperatorEnum.IN, "15");
                LogicalTupleFilter logicalAndFilter = and(andFilter1, andFilter2);

                TupleFilter andFilter3 = compare(col, FilterOperatorEnum.EQ, start2);
                TupleFilter andFilter4 = compare(col2, FilterOperatorEnum.LT, "35");
                LogicalTupleFilter logicalAndFilter2 = or(andFilter3, andFilter4);

                LogicalTupleFilter finalFilter = or(logicalAndFilter, logicalAndFilter2);
                SegmentPruner segmentPruner = new SegmentPruner(finalFilter);
                Assert.assertTrue(segmentPruner.check(seg));
                Assert.assertTrue(segmentPruner.check(seg2));
            }

            {
                TupleFilter andFilter1 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter2 = compare(col2, FilterOperatorEnum.EQ, "35");
                LogicalTupleFilter logicalAndFilter = or(andFilter1, andFilter2);

                TupleFilter andFilter3 = compare(col, FilterOperatorEnum.EQ, start2);
                TupleFilter andFilter4 = compare(col2, FilterOperatorEnum.LT, "35");
                LogicalTupleFilter logicalAndFilter2 = or(andFilter3, andFilter4);

                LogicalTupleFilter finalFilter1 = or(logicalAndFilter, logicalAndFilter2);

                TupleFilter andFilter5 = compare(col, FilterOperatorEnum.EQ, start);
                TupleFilter andFilter6 = compare(col2, FilterOperatorEnum.IN, "15");
                LogicalTupleFilter logicalAndFilter3 = and(andFilter5, andFilter6);

                TupleFilter andFilter7 = compare(col, FilterOperatorEnum.EQ, start2);
                TupleFilter andFilter8 = compare(col2, FilterOperatorEnum.LT, "35");
                LogicalTupleFilter logicalAndFilter4 = or(andFilter7, andFilter8);

                LogicalTupleFilter finalFilter2 = or(logicalAndFilter3, logicalAndFilter4);

                LogicalTupleFilter finalFinalFilter = or(finalFilter1, finalFilter2);

                SegmentPruner segmentPruner = new SegmentPruner(finalFinalFilter);
                Assert.assertTrue(segmentPruner.check(seg));
                Assert.assertTrue(segmentPruner.check(seg2));
            }
        }
    }

    @Test
    public void testPruneSegWithFilterIN() {
        // legacy cube segments does not have DimensionRangeInfo, but with TSRange can do some pruning
        CubeInstance cube = CubeManager.getInstance(getTestConfig())
                .getCube("test_kylin_cube_without_slr_left_join_ready_2_segments");
        TblColRef col = cube.getModel().findColumn("TEST_KYLIN_FACT.CAL_DT");
        CubeSegment seg = cube.getSegments(SegmentStatusEnum.READY).get(0);
        TSRange tsRange = seg.getTSRange();
        String start = DateFormat.formatToTimeStr(tsRange.start.v, "yyyy-MM-dd");
        CubeSegment seg2 = cube.getSegments(SegmentStatusEnum.READY).get(1);
        TSRange tsRange2 = seg2.getTSRange();
        try (SetAndUnsetSystemProp sns = new SetAndUnsetSystemProp("kylin.query.skip-empty-segments", "false")) {

            {
                TupleFilter inFilter = new ConstantTupleFilter(Sets.newHashSet(start,
                        DateFormat.formatToTimeStr(tsRange2.end.v + 1000 * 60 * 60 * 24L, "yyyy-MM-dd")));
                TupleFilter filter = compare(col, FilterOperatorEnum.IN, inFilter);
                SegmentPruner segmentPruner = new SegmentPruner(filter);
                Assert.assertTrue(segmentPruner.check(seg));
                Assert.assertFalse(segmentPruner.check(seg2));

            }
        }
    }
}
