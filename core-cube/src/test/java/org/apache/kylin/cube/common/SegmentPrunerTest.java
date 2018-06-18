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

import static org.apache.kylin.metadata.filter.TupleFilter.compare;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.SetAndUnsetSystemProp;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

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
    public void testDimensionRangeCheck() {
        CubeSegment cubeSegment = cube.getSegments().getFirstSegment();

        //integer
        TblColRef qtyCol = cube.getModel().findColumn("V_LINEORDER.LO_QUANTITY");
        TupleFilter constFilter_LO_QUANTITY0 = new ConstantTupleFilter(Sets.newHashSet("8", "18", "28"));//between min and max value
        TupleFilter constFilter_LO_QUANTITY1 = new ConstantTupleFilter("1");//min value
        TupleFilter constFilter_LO_QUANTITY2 = new ConstantTupleFilter("50");//max value
        TupleFilter constFilter_LO_QUANTITY3 = new ConstantTupleFilter("0");//lt min value
        TupleFilter constFilter_LO_QUANTITY4 = new ConstantTupleFilter("200");//gt max value
        TupleFilter constFilter_LO_QUANTITY5 = new ConstantTupleFilter(Sets.newHashSet("51", "52", "53"));//gt max values

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
}
