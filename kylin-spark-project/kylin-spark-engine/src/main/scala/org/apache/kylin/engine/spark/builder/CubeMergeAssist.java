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

package org.apache.kylin.engine.spark.builder;

import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CubeMergeAssist implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(CubeMergeAssist.class);
    private static final int DEFAULT_BUFFER_SIZE = 256;
    private LayoutEntity layout;
    private SegmentInfo newSegment;
    private List<SegmentInfo> toMergeSegments;
    private SparkSession ss;
    final private List<LayoutEntity> toMergeCuboids = new ArrayList<>();

    public void setSs(SparkSession ss) {
        this.ss = ss;
    }

    public void setLayout(LayoutEntity layout) {
        this.layout = layout;
    }

    public LayoutEntity getLayout() {
        return this.layout;
    }

    public List<LayoutEntity> getCuboids() {
        return this.toMergeCuboids;
    }

    public void addCuboid(LayoutEntity cuboid) {
        toMergeCuboids.add(cuboid);
    }

    public void setToMergeSegments(List<SegmentInfo> segments) {
        this.toMergeSegments = segments;
    }

    public void setNewSegment(SegmentInfo segment) {
        this.newSegment = segment;
    }

    public SegmentInfo getSegment() {
        return newSegment;
    }

    public Dataset<Row> merge(KylinConfig config, String cubeId) {
        Dataset<Row> mergeDataset = null;
        for (int i = 0; i < toMergeSegments.size(); i++) {
            Dataset<Row> layoutDataset = StorageFactory
                    .createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                    .getFrom(PathManager.getParquetStoragePath(config, cubeId,
                            toMergeSegments.get(i).name(), toMergeSegments.get(i).identifier(), String.valueOf(layout.getId())), ss);

            if (mergeDataset == null) {
                mergeDataset = layoutDataset;
            } else
                mergeDataset = mergeDataset.union(layoutDataset);
        }
        return mergeDataset;

    }

}
