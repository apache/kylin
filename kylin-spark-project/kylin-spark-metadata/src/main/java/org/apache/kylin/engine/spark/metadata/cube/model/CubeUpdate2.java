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

import org.apache.kylin.metadata.realization.RealizationStatusEnum;

public class CubeUpdate2 {
    private final String cubeId;

    private DataSegment[] toAddSegs = null;

    private DataSegment[] toRemoveSegs = null;

    private DataSegment[] toUpdateSegs = null;

    private DataLayout[] toAddOrUpdateLayouts = null;

    private DataLayout[] toRemoveLayouts = null;

    private RealizationStatusEnum status;

    private String description;

    private String owner;

    private int cost = -1;

    public CubeUpdate2(String cubeId) {
        this.cubeId = cubeId;
    }

    public CubeUpdate2 setToAddSegs(DataSegment... toAddSegs) {
        //        for (DataSegment seg : toAddSegs)
        //            seg.checkIsNotCachedAndShared();

        this.toAddSegs = toAddSegs;
        return this;
    }

    public CubeUpdate2 setToRemoveSegs(DataSegment... toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public CubeUpdate2 setToRemoveSegsWithArray(DataSegment[] toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public CubeUpdate2 setToUpdateSegs(DataSegment... toUpdateSegs) {
        //        for (DataSegment seg : toUpdateSegs)
        //            seg.checkIsNotCachedAndShared();

        this.toUpdateSegs = toUpdateSegs;
        return this;
    }

    public void setToAddOrUpdateLayouts(DataLayout... toAddCuboids) {
        //        for (DataLayout cuboid : toAddCuboids)
        //            cuboid.checkIsNotCachedAndShared();

        this.toAddOrUpdateLayouts = toAddCuboids;
    }

    public void setToRemoveLayouts(DataLayout... toRemoveLayouts) {
        this.toRemoveLayouts = toRemoveLayouts;
    }
}
