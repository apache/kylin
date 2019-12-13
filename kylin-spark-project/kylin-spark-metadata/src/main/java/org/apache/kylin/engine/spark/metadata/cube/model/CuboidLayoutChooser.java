/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import java.util.Comparator;
import java.util.Optional;

public class CuboidLayoutChooser {
    public static LayoutEntity selectLayoutForBuild(DataSegment segment, IndexEntity entity) {
        Optional<LayoutEntity> candidate = segment.getCube().getAllIndexes().stream() //
                .filter(index -> index.fullyDerive(entity)) //
                .flatMap(index -> index.getLayouts().stream()) //
                .filter(layout -> (segment.getLayout(layout.getId()) != null)) //
                .min(Comparator.comparingLong(o -> getRows(o, segment))); //
        return candidate.orElse(null);
    }

    private static long getRows(LayoutEntity o1, DataSegment segment) {
        return segment.getLayout(o1.getId()).getRows();
    }
}