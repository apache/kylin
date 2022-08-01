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

package org.apache.kylin.metadata.cube.cuboid;

import java.util.Comparator;
import java.util.Objects;

import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import lombok.val;

public class NCuboidLayoutChooser {

    public static LayoutEntity selectLayoutForBuild(NDataSegment segment, IndexEntity entity) {
        val candidate = segment.getIndexPlan().getAllIndexes().stream() //
                .filter(index -> index.fullyDerive(entity)) //
                .flatMap(index -> index.getLayouts().stream()) //
                .filter(layout -> (segment.getLayout(layout.getId()) != null)) //
                .min(Comparator.comparingLong(layout -> segment.getLayout(layout.getId()).getRows())); //
        return candidate.orElse(null);
    }

    public static LayoutEntity selectLayoutForBuild(NDataSegment segment, IndexEntity index, Long partitionId) {
        val candidate = segment.getIndexPlan().getAllIndexes().stream() //
                .filter(parent -> parent.fullyDerive(index)) //
                .flatMap(parent -> parent.getLayouts().stream()) //
                .filter(parent -> Objects.nonNull(segment.getLayout(parent.getId()))) //
                .filter(parent -> Objects.nonNull(segment.getLayout(parent.getId()).getDataPartition(partitionId))) //
                .min(Comparator.comparingLong(parent -> //
                segment.getLayout(parent.getId()).getDataPartition(partitionId).getRows()));
        return candidate.orElse(null);
    }
}
