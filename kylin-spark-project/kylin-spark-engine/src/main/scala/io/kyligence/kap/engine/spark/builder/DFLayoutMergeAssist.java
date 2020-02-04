/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.builder;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
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

public class DFLayoutMergeAssist implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(DFLayoutMergeAssist.class);
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
                            toMergeSegments.get(i).id(), String.valueOf(layout.getId())), ss);

            if (mergeDataset == null) {
                mergeDataset = layoutDataset;
            } else
                mergeDataset = mergeDataset.union(layoutDataset);
        }
        return mergeDataset;

    }

}
