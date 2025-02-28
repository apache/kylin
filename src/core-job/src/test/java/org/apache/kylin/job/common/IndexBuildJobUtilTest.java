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

package org.apache.kylin.job.common;

import static org.apache.kylin.metadata.cube.model.IndexEntity.INDEX_ID_STEP;
import static org.apache.kylin.metadata.cube.model.IndexEntity.TABLE_INDEX_START_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.Segments;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class IndexBuildJobUtilTest {

    private NDataflow df;
    private IndexPlan indexPlan;

    @BeforeEach
    void setUp() {
        df = mock(NDataflow.class);
        indexPlan = mock(IndexPlan.class);
    }

    @Test
    public void testPruneTBDelLayouts_BaseIndex() {
        LayoutEntity layout1 = new LayoutEntity();
        layout1.setId(TABLE_INDEX_START_ID + 1);
        layout1.setColOrder(Arrays.asList(1, 2, 3));
        layout1.setBase(true);
        layout1.setToBeDeleted(true);

        LayoutEntity layout2 = new LayoutEntity();
        layout2.setId(1);
        layout2.setColOrder(Arrays.asList(1, 2, 3, 1000));
        layout2.setBase(true);
        layout2.setToBeDeleted(true);

        LayoutEntity layout11 = new LayoutEntity();
        layout11.setId(TABLE_INDEX_START_ID + INDEX_ID_STEP + 1);
        layout11.setColOrder(Arrays.asList(1, 2, 3, 4));
        layout11.setBase(true);
        layout11.setToBeDeleted(false);

        LayoutEntity layout22 = new LayoutEntity();
        layout22.setId(INDEX_ID_STEP + 1);
        layout22.setColOrder(Arrays.asList(1, 2, 3, 4, 1000));
        layout22.setBase(true);
        layout22.setToBeDeleted(false);

        NDataSegment seg1 = mock(NDataSegment.class);
        when(seg1.getId()).thenReturn("seg1");
        when(seg1.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout1.getId(), layout2.getId())));

        NDataSegment seg2 = mock(NDataSegment.class);
        when(seg2.getId()).thenReturn("seg2");
        when(seg2.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout1.getId(), layout2.getId())));

        NDataSegment seg3 = mock(NDataSegment.class);
        when(seg3.getId()).thenReturn("seg3");
        when(seg3.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout1.getId())));

        when(indexPlan.getAllLayouts()).thenReturn(Arrays.asList(layout1, layout2, layout11, layout22));
        when(df.getSegments()).thenReturn(new Segments<>(Arrays.asList(seg1, seg2, seg3)));

        Set<LayoutEntity> tbDelLayouts = new LinkedHashSet<>(Arrays.asList(layout1, layout2));
        Set<LayoutEntity> tbProLayouts = new LinkedHashSet<>(Arrays.asList(layout11, layout22));

        {
            List<NDataSegment> readySegs = Arrays.asList(seg1, seg2);

            Set<LayoutEntity> result = IndexBuildJobUtil.pruneTBDelLayouts(tbDelLayouts, tbProLayouts, df, indexPlan,
                    readySegs);
            assertEquals(1, result.size());
            result.stream().findFirst().ifPresent(layoutEntity -> assertEquals(layout2.getId(), layoutEntity.getId()));
        }

        {
            List<NDataSegment> readySegs = Arrays.asList(seg1, seg2, seg3);

            Set<LayoutEntity> result = IndexBuildJobUtil.pruneTBDelLayouts(tbDelLayouts, tbProLayouts, df, indexPlan,
                    readySegs);
            assertEquals(2, result.size());
            List<LayoutEntity> deleted = result.stream().sorted(layoutIdComparator).collect(Collectors.toList());
            assertEquals(layout2.getId(), deleted.get(0).getId());
            assertEquals(layout1.getId(), deleted.get(1).getId());
        }
    }

    @Test
    public void testPruneTBDelLayouts_RegularIndex() {
        LayoutEntity layout1 = new LayoutEntity();
        layout1.setId(TABLE_INDEX_START_ID + 1);
        layout1.setColOrder(Arrays.asList(1, 2, 3));
        layout1.setBase(true);
        layout1.setToBeDeleted(true);

        LayoutEntity layout2 = new LayoutEntity();
        layout2.setId(1);
        layout2.setColOrder(Arrays.asList(1, 2, 1000));
        layout2.setBase(false);
        layout2.setToBeDeleted(true);

        LayoutEntity layout11 = new LayoutEntity();
        layout11.setId(TABLE_INDEX_START_ID + INDEX_ID_STEP + 1);
        layout11.setColOrder(Arrays.asList(1, 2, 3, 4));
        layout11.setBase(true);
        layout11.setToBeDeleted(false);

        LayoutEntity layout22 = new LayoutEntity();
        layout22.setId(INDEX_ID_STEP + 1);
        layout22.setColOrder(Arrays.asList(1, 2, 4, 1000));
        layout22.setBase(false);
        layout22.setToBeDeleted(false);

        NDataSegment seg1 = mock(NDataSegment.class);
        when(seg1.getId()).thenReturn("seg1");

        NDataSegment seg2 = mock(NDataSegment.class);
        when(seg2.getId()).thenReturn("seg2");

        NDataSegment seg3 = mock(NDataSegment.class);
        when(seg3.getId()).thenReturn("seg3");

        when(indexPlan.getAllLayouts()).thenReturn(Arrays.asList(layout1, layout2, layout11, layout22));
        when(df.getSegments()).thenReturn(new Segments<>(Arrays.asList(seg1, seg2, seg3)));

        Set<LayoutEntity> tbDelLayouts = new LinkedHashSet<>(Arrays.asList(layout1, layout2));
        Set<LayoutEntity> tbProLayouts = new LinkedHashSet<>(Arrays.asList(layout11, layout22));

        {
            when(seg1.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout2.getId(), layout22.getId())));
            when(seg2.getLayoutIds()).thenReturn(new LinkedHashSet<>(
                    Arrays.asList(layout1.getId(), layout2.getId(), layout11.getId(), layout22.getId())));
            when(seg3.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout2.getId())));
            List<NDataSegment> readySegs = Arrays.asList(seg3);

            Set<LayoutEntity> result = IndexBuildJobUtil.pruneTBDelLayouts(tbDelLayouts, tbProLayouts, df, indexPlan,
                    readySegs);
            assertEquals(1, result.size());
            result.stream().findFirst().ifPresent(layoutEntity -> assertEquals(layout1.getId(), layoutEntity.getId()));
        }

        {
            when(seg1.getLayoutIds()).thenReturn(new LinkedHashSet<>(
                    Arrays.asList(layout1.getId(), layout2.getId(), layout11.getId(), layout22.getId())));
            when(seg2.getLayoutIds()).thenReturn(new LinkedHashSet<>(
                    Arrays.asList(layout1.getId(), layout2.getId(), layout11.getId(), layout22.getId())));
            when(seg3.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout2.getId())));
            List<NDataSegment> readySegs = Arrays.asList(seg3);

            Set<LayoutEntity> result = IndexBuildJobUtil.pruneTBDelLayouts(tbDelLayouts, tbProLayouts, df, indexPlan,
                    readySegs);
            assertEquals(2, result.size());
            List<LayoutEntity> deleted = result.stream().sorted(layoutIdComparator).collect(Collectors.toList());
            assertEquals(layout2.getId(), deleted.get(0).getId());
            assertEquals(layout1.getId(), deleted.get(1).getId());
        }
    }

    @Test
    public void testPruneTBDelLayouts_RegularIndex2_ForSegmentLayoutsNotFull() {
        LayoutEntity layout1 = new LayoutEntity();
        layout1.setId(TABLE_INDEX_START_ID + 1);
        layout1.setColOrder(Arrays.asList(1, 2, 3));
        layout1.setBase(true);
        layout1.setToBeDeleted(true);

        LayoutEntity layout2 = new LayoutEntity();
        layout2.setId(1);
        layout2.setColOrder(Arrays.asList(1, 2, 1000));
        layout2.setBase(true);
        layout2.setToBeDeleted(true);

        LayoutEntity layout3 = new LayoutEntity();
        layout3.setId(INDEX_ID_STEP * 2 + 1);
        layout3.setColOrder(Arrays.asList(2, 3, 1000));
        layout3.setBase(false);
        layout3.setToBeDeleted(true);

        LayoutEntity layout11 = new LayoutEntity();
        layout11.setId(TABLE_INDEX_START_ID + INDEX_ID_STEP + 1);
        layout11.setColOrder(Arrays.asList(1, 2, 3, 4));
        layout11.setBase(true);
        layout11.setToBeDeleted(false);

        LayoutEntity layout22 = new LayoutEntity();
        layout22.setId(INDEX_ID_STEP + 1);
        layout22.setColOrder(Arrays.asList(1, 2, 3, 4, 1000));
        layout22.setBase(true);
        layout22.setToBeDeleted(false);

        LayoutEntity layout33 = new LayoutEntity();
        layout33.setId(INDEX_ID_STEP * 3 + 1);
        layout33.setColOrder(Arrays.asList(2, 3, 4, 1000));
        layout33.setBase(false);
        layout33.setToBeDeleted(false);

        NDataSegment seg1 = mock(NDataSegment.class);
        when(seg1.getId()).thenReturn("seg1");

        NDataSegment seg2 = mock(NDataSegment.class);
        when(seg2.getId()).thenReturn("seg2");

        NDataSegment seg3 = mock(NDataSegment.class);
        when(seg3.getId()).thenReturn("seg3");

        when(indexPlan.getAllLayouts())
                .thenReturn(Arrays.asList(layout1, layout2, layout3, layout11, layout22, layout33));
        when(df.getSegments()).thenReturn(new Segments<>(Arrays.asList(seg1, seg2, seg3)));

        Set<LayoutEntity> tbDelLayouts = new LinkedHashSet<>(Arrays.asList(layout1, layout2, layout3));
        Set<LayoutEntity> tbProLayouts = new LinkedHashSet<>(Arrays.asList(layout11, layout22, layout33));

        {
            when(seg1.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout1.getId(), layout3.getId(),
                    layout11.getId(), layout22.getId(), layout33.getId())));
            when(seg2.getLayoutIds()).thenReturn(new LinkedHashSet<>(Arrays.asList(layout1.getId(), layout2.getId(),
                    layout3.getId(), layout11.getId(), layout22.getId(), layout33.getId())));
            when(seg3.getLayoutIds())
                    .thenReturn(new LinkedHashSet<>(Arrays.asList(layout1.getId(), layout2.getId(), layout3.getId())));
            List<NDataSegment> readySegs = Arrays.asList(seg3);

            Set<LayoutEntity> result = IndexBuildJobUtil.pruneTBDelLayouts(tbDelLayouts, tbProLayouts, df, indexPlan,
                    readySegs);
            assertEquals(3, result.size());
            List<LayoutEntity> deleted = result.stream().sorted(layoutIdComparator).collect(Collectors.toList());
            assertEquals(layout2.getId(), deleted.get(0).getId());
            assertEquals(layout3.getId(), deleted.get(1).getId());
            assertEquals(layout1.getId(), deleted.get(2).getId());
        }
    }

    /**
     * A benchmark test can be triggered manually in local.
     * <p>Beware of mocked & spied objects as these may greatly slow down benchmark performance with OOM happened.
     * <p>The time usage for {@link IndexBuildJobUtil#pruneTBDelLayouts} in the test should be around 300 ~ 600 ms.
     */
    @Test
    @Disabled("A benchmark test triggered manually")
    public void testPruneTBDelLayouts_Performance() {
        List<LayoutEntity> oldBaseTableLayouts = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            LayoutEntity layout = new LayoutEntity();
            layout.setId(TABLE_INDEX_START_ID + i + 1);
            layout.setBase(true);
            layout.setToBeDeleted(true);
            layout.setStorageType(i); // generating different hashcode for comparison
            oldBaseTableLayouts.add(layout);
        }

        List<LayoutEntity> newBaseTableLayouts = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            LayoutEntity layout = new LayoutEntity();
            layout.setId(TABLE_INDEX_START_ID + INDEX_ID_STEP + i + 1);
            layout.setBase(true);
            layout.setToBeDeleted(false);
            layout.setStorageType(i + 400); // generating different hashcode for comparison
            newBaseTableLayouts.add(layout);
        }

        List<LayoutEntity> oldRegularLayouts = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            LayoutEntity layout = new LayoutEntity();
            layout.setId(INDEX_ID_STEP + i + 1);
            layout.setBase(false);
            layout.setToBeDeleted(true);
            layout.setStorageType(i + 800); // generating different hashcode for comparison
            oldRegularLayouts.add(layout);
        }

        List<LayoutEntity> newRegularLayouts = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            LayoutEntity layout = new LayoutEntity();
            layout.setId(INDEX_ID_STEP * 2 + i + 1);
            layout.setBase(false);
            layout.setToBeDeleted(false);
            layout.setStorageType(i + 1200); // generating different hashcode for comparison
            newRegularLayouts.add(layout);
        }

        Set<LayoutEntity> tbDelLayouts = new LinkedHashSet<>();
        tbDelLayouts.addAll(oldBaseTableLayouts);
        tbDelLayouts.addAll(oldRegularLayouts);

        Set<LayoutEntity> tbProLayouts = new LinkedHashSet<>();
        tbProLayouts.addAll(newBaseTableLayouts);
        tbProLayouts.addAll(newRegularLayouts);

        List<LayoutEntity> allLayouts = new ArrayList<>();
        allLayouts.addAll(oldBaseTableLayouts);
        allLayouts.addAll(oldRegularLayouts);
        allLayouts.addAll(newBaseTableLayouts);
        allLayouts.addAll(newRegularLayouts);

        Map<Long, NDataLayout> allLayoutsMap = allLayouts.stream().map(le -> {
            NDataLayout ndl = new NDataLayout();
            ndl.setLayoutId(le.getId());
            return ndl;
        }).collect(Collectors.toMap(NDataLayout::getLayoutId, Function.identity()));

        Map<Long, NDataLayout> oldLayoutsMap = tbDelLayouts.stream().map(le -> {
            NDataLayout ndl = new NDataLayout();
            ndl.setLayoutId(le.getId());
            return ndl;
        }).collect(Collectors.toMap(NDataLayout::getLayoutId, Function.identity()));

        List<NDataSegment> segments = new ArrayList<>();
        for (int i = 0; i < 299; i++) {
            NDataSegment seg = new NDataSegment();
            seg.setId("seg" + (i + 1));
            NDataSegment.LayoutInfoOnlyForTest info = seg.new LayoutInfoOnlyForTest(allLayoutsMap);
            ReflectionTestUtils.setField(seg, "layoutInfo", info);
            segments.add(seg);
        }
        List<NDataSegment> readySegs = new ArrayList<>();
        for (int i = 299; i < 300; i++) {
            NDataSegment seg = new NDataSegment();
            seg.setId("seg" + (i + 1));
            NDataSegment.LayoutInfoOnlyForTest info = seg.new LayoutInfoOnlyForTest(oldLayoutsMap);
            ReflectionTestUtils.setField(seg, "layoutInfo", info);
            segments.add(seg);
            readySegs.add(seg);
        }

        when(indexPlan.getAllLayouts()).thenReturn(allLayouts);
        when(df.getSegments()).thenReturn(new Segments<>(segments));

        long start = System.currentTimeMillis();
        Set<LayoutEntity> result = IndexBuildJobUtil.pruneTBDelLayouts(tbDelLayouts, tbProLayouts, df, indexPlan,
                readySegs);
        System.out.println("Time usage: " + (System.currentTimeMillis() - start) + " ms");
        assertEquals(800, result.size());
    }

    private static final Comparator<LayoutEntity> layoutIdComparator = (a, b) -> {
        if (a.getId() < b.getId())
            return -1;
        else if (a.getId() > b.getId())
            return 1;
        return 0;
    };
}
