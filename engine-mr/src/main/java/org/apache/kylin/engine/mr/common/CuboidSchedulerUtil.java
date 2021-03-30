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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.cuboid.TreeCuboidScheduler;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class CuboidSchedulerUtil {

    public static CuboidScheduler getCuboidSchedulerByMode(CubeSegment segment, String cuboidModeName) {
        if (cuboidModeName == null)
            return segment.getCuboidScheduler();
        else
            return getCuboidSchedulerByMode(segment, CuboidModeEnum.getByModeName(cuboidModeName));
    }

    public static CuboidScheduler getCuboidSchedulerByMode(CubeSegment segment, CuboidModeEnum cuboidMode) {
        if (cuboidMode == CuboidModeEnum.CURRENT || cuboidMode == null)
            return segment.getCuboidScheduler();
        else
            return getCuboidScheduler(segment, segment.getCubeInstance().getCuboidsByMode(cuboidMode));
    }

    public static CuboidScheduler getCuboidScheduler(CubeSegment segment, Set<Long> cuboidSet) {
        try {
            Map<Long, Long> cuboidsWithRowCnt = CuboidStatsReaderUtil.readCuboidStatsFromSegment(cuboidSet, segment);
            Comparator<Long> comparator = cuboidsWithRowCnt == null ? Cuboid.cuboidSelectComparator
                    : new TreeCuboidScheduler.CuboidCostComparator(cuboidsWithRowCnt);
            return new TreeCuboidScheduler(segment.getCubeDesc(), Lists.newArrayList(cuboidSet), comparator);
        } catch (IOException e) {
            throw new RuntimeException("Fail to cube stats for segment" + segment + " due to " + e);
        }
    }
}
