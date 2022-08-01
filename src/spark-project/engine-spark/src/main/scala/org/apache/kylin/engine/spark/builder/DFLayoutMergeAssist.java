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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.datasource.storage.StorageStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DFLayoutMergeAssist implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(DFLayoutMergeAssist.class);
    private static final int DEFAULT_BUFFER_SIZE = 256;
    final private List<NDataLayout> toMergeCuboids = new ArrayList<>();
    private LayoutEntity layout;
    private NDataSegment newSegment;
    private List<NDataSegment> toMergeSegments;
    private SparkSession ss;

    public void setSs(SparkSession ss) {
        this.ss = ss;
    }

    public LayoutEntity getLayout() {
        return this.layout;
    }

    public void setLayout(LayoutEntity layout) {
        this.layout = layout;
    }

    public List<NDataLayout> getCuboids() {
        return this.toMergeCuboids;
    }

    public void addCuboid(NDataLayout cuboid) {
        toMergeCuboids.add(cuboid);
    }

    public void setToMergeSegments(List<NDataSegment> segments) {
        this.toMergeSegments = segments;
    }

    public void setNewSegment(NDataSegment segment) {
        this.newSegment = segment;
    }

    public Dataset<Row> merge() {
        Dataset<Row> mergeDataset = null;
        for (int i = 0; i < toMergeCuboids.size(); i++) {
            NDataLayout nDataLayout = toMergeCuboids.get(i);
            ss.sparkContext().setJobDescription("Union segments layout " + nDataLayout.getLayoutId());
            Dataset<Row> layoutDataset = getLayoutDS(nDataLayout);
            if (mergeDataset == null) {
                mergeDataset = layoutDataset;
            } else
                mergeDataset = mergeDataset.union(layoutDataset);

            ss.sparkContext().setJobDescription(null);
        }
        return mergeDataset;
    }

    private Dataset<Row> getLayoutDS(NDataLayout dataLayout) {
        final NDataSegment dataSegment = dataLayout.getSegDetails().getDataSegment();
        if (CollectionUtils.isEmpty(dataLayout.getMultiPartition())) {
            return StorageStoreUtils.toDF(dataSegment, dataLayout.getLayout(), ss);
        }
        Dataset<Row> mergedDS = null;
        for (LayoutPartition partition : dataLayout.getMultiPartition()) {
            Dataset<Row> partitionDS = StorageStoreUtils.toDF(dataSegment, //
                    dataLayout.getLayout(), //
                    partition.getPartitionId(), ss);
            if (Objects.isNull(mergedDS)) {
                mergedDS = partitionDS;
            } else {
                mergedDS = mergedDS.union(partitionDS);
            }
        }
        return mergedDS;
    }

}
