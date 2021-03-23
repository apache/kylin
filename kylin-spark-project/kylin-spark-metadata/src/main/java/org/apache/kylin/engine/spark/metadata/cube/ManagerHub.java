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

package org.apache.kylin.engine.spark.metadata.cube;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.engine.spark.metadata.MetadataConverter;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;

import java.io.IOException;

public class ManagerHub {

    private ManagerHub() {
    }

    public static SegmentInfo getSegmentInfo(KylinConfig kylinConfig, String cubeId, String segmentId) {
        return getSegmentInfo(kylinConfig, cubeId, segmentId, CuboidModeEnum.CURRENT);
    }

    public static SegmentInfo getSegmentInfo(KylinConfig kylinConfig, String cubeId, String segmentId, CuboidModeEnum cuboidMode) {
        CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCubeByUuid(cubeId);
        CubeSegment segment = cubeInstance.getSegmentById(segmentId);
        return MetadataConverter.getSegmentInfo(CubeManager.getInstance(kylinConfig).getCubeByUuid(cubeId),
                segment.getUuid(), segment.getName(), segment.getStorageLocationIdentifier(), cuboidMode);
    }

    public static CubeInstance updateSegment(KylinConfig kylinConfig, SegmentInfo segmentInfo) throws IOException {
        return CubeManager.getInstance(kylinConfig).updateCube(MetadataConverter.getCubeUpdate(segmentInfo));
    }
}
