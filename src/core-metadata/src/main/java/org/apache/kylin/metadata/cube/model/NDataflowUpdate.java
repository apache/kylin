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

import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class NDataflowUpdate {

    @Getter
    private final String dataflowId;

    @Getter
    private NDataSegment[] toAddSegs = null;
    @Getter
    private NDataSegment[] toRemoveSegs = null;
    @Getter
    private NDataSegment[] toUpdateSegs = null;

    @Getter
    private NDataLayout[] toAddOrUpdateLayouts = null;
    @Getter
    private NDataLayout[] toRemoveLayouts = null;

    @Accessors(chain = true)
    @Setter
    @Getter
    private RealizationStatusEnum status;

    @Accessors(chain = true)
    @Setter
    @Getter
    private int cost = -1;

    public NDataflowUpdate(String dataflowId) {
        this.dataflowId = dataflowId;
    }

    public NDataflowUpdate setToAddSegs(NDataSegment... toAddSegs) {
        for (NDataSegment seg : toAddSegs)
            seg.checkIsNotCachedAndShared();

        this.toAddSegs = toAddSegs;
        return this;
    }

    public NDataflowUpdate setToRemoveSegs(NDataSegment... toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public NDataflowUpdate setToRemoveSegsWithArray(NDataSegment[] toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public NDataflowUpdate setToUpdateSegs(NDataSegment... toUpdateSegs) {
        for (NDataSegment seg : toUpdateSegs)
            seg.checkIsNotCachedAndShared();

        this.toUpdateSegs = toUpdateSegs;
        return this;
    }

    public void setToAddOrUpdateLayouts(NDataLayout... toAddCuboids) {
        for (NDataLayout cuboid : toAddCuboids)
            cuboid.checkIsNotCachedAndShared();

        this.toAddOrUpdateLayouts = toAddCuboids;
    }

    public void setToRemoveLayouts(NDataLayout... toRemoveLayouts) {
        this.toRemoveLayouts = toRemoveLayouts;
    }

}
