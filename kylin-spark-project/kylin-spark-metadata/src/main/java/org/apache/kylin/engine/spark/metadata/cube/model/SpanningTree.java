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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;

public abstract class SpanningTree implements Serializable {
    final protected Collection<LayoutEntity> cuboids;

    public SpanningTree( Collection<LayoutEntity> cuboids) {
        this.cuboids = cuboids;
    }

    abstract public boolean isValid(long cuboidId);

    abstract public int getCuboidCount();

    abstract public Collection<LayoutEntity> getRootIndexEntities();


    abstract public LayoutEntity getLayoutEntity(long cuboidId);


    abstract public void decideTheNextLayer(Collection<LayoutEntity> currentLayer, SegmentInfo segment);

    abstract public Collection<LayoutEntity> getChildrenByIndexPlan(LayoutEntity parent);

    abstract public Collection<LayoutEntity> getAllIndexEntities();

    public static class TreeNode implements Serializable {
        @JsonProperty("cuboid")
        protected final LayoutEntity indexEntity;

        @JsonProperty("children")
        protected final ArrayList<TreeNode> children = Lists.newArrayList();

        @JsonProperty("level")
        protected int level;

        protected transient TreeNode parent;
        protected transient List<LayoutEntity> parentCandidates;
        protected transient boolean hasBeenDecided = false;

        public TreeNode(LayoutEntity indexEntity) {
            this.indexEntity = indexEntity;
        }

        @Override
        public String toString() {
            return "level:" + level + ", node:" + indexEntity.getId() + //
                    ", dim:" + indexEntity.getOrderedDimensions().keySet().toString() + //
                    ", measure:" + indexEntity.getOrderedMeasures().keySet().toString() + //
                    ", children:{" + children.toString() + "}";//
        }
    }
}
