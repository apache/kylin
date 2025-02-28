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

package org.apache.kylin.metadata.cube.model;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Range;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo
public class NDataLayoutDetailsManagerTest {

    private static final String PROJECT = "default";

    @Test
    public void testBasic() {
        NDataLayoutDetailsManager layoutFragmentMgr = NDataLayoutDetailsManager.getInstance(getTestConfig(), PROJECT);
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), PROJECT).listAllDataflows().get(0);
        LayoutEntity layoutEntity = dataflow.getIndexPlan().getAllLayouts().get(0);
        layoutFragmentMgr.updateLayoutDetails(dataflow.getId(), layoutEntity.getId(), (copyForWrite) -> {
            copyForWrite.getFragmentRangeSet().add(Range.closedOpen(201L, 300L));
            copyForWrite.getFragmentRangeSet().add(Range.closedOpen(100L, 200L));
            copyForWrite.getFragmentRangeSet().add(Range.closedOpen(0L, 100L));
        });
        NDataLayoutDetails layoutFragment1 = layoutFragmentMgr.getNDataLayoutDetails(dataflow.getId(),
                layoutEntity.getId());
        ArrayList<Range<Long>> ranges1 = Lists
                .newArrayList(layoutFragment1.getFragmentRangeSet().asRanges().iterator());
        Assertions.assertEquals(Lists.newArrayList(Range.closedOpen(0L, 200L), Range.closedOpen(201L, 300L)), ranges1);
        layoutFragmentMgr.updateLayoutDetails(dataflow.getId(), layoutEntity.getId(), (copyForWrite) -> {
            copyForWrite.getFragmentRangeSet().remove(Range.closedOpen(50L, 100L));
        });
        NDataLayoutDetails layoutFragment2 = layoutFragmentMgr.getNDataLayoutDetails(dataflow.getId(),
                layoutEntity.getId());
        ArrayList<Range<Long>> ranges2 = Lists
                .newArrayList(layoutFragment2.getFragmentRangeSet().asRanges().iterator());
        Assertions.assertEquals(Lists.newArrayList(Range.closedOpen(0L, 50L), Range.closedOpen(100L, 200L),
                Range.closedOpen(201L, 300L)), ranges2);
        layoutFragmentMgr.removeDetails(dataflow.getId(),
                new HashSet<>(Collections.singletonList(layoutEntity.getId())));
        Assertions.assertNull(layoutFragmentMgr.getNDataLayoutDetails(dataflow.getId(), layoutEntity.getId()));
    }

}
