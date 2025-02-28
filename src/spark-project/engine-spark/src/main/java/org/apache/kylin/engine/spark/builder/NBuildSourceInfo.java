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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NBuildSourceInfo {
    protected static final Logger logger = LoggerFactory.getLogger(NBuildSourceInfo.class);

    private Dataset<Row> flattableDS;
    private String viewFactTablePath;
    private SparkSession ss;
    private long byteSize;
    private long count;
    private long layoutId;
    private Dataset<Row> parentDF;
    private Collection<IndexEntity> toBuildCuboids = new LinkedHashSet<>();
    private List<TblColRef> allColumns;

    public List<TblColRef> getAllColumns() {
        return allColumns;
    }

    public void setAllColumns(List<TblColRef> allColumns) {
        this.allColumns = allColumns;
    }

    public long getByteSize() {
        return byteSize;
    }

    public void setByteSize(long byteSize) {
        this.byteSize = byteSize;
    }

    public Dataset<Row> getFlattableDS() {
        return flattableDS;
    }

    public void setFlattableDS(Dataset<Row> flattableDS) {
        this.flattableDS = flattableDS;
    }

    public Dataset<Row> getParentDS() {
        if (parentDF != null) {
            Preconditions.checkNotNull(ss, "SparkSession is null is NBuildSourceInfo.");
            return parentDF;
        } else {
            Preconditions.checkState(flattableDS != null, "Path and DS can no be empty at the same time.");
            logger.info("parent storage path not exists, use flattable dataset.");
            return flattableDS;
        }
    }

    public void setSparkSession(SparkSession ss) {
        this.ss = ss;
    }

    public String getViewFactTablePath() {
        return viewFactTablePath;
    }

    public void setViewFactTablePath(String viewFactTablePath) {
        this.viewFactTablePath = viewFactTablePath;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(long layoutId) {
        this.layoutId = layoutId;
    }

    public Collection<IndexEntity> getToBuildCuboids() {
        return this.toBuildCuboids;
    }

    public void setToBuildCuboids(Collection<IndexEntity> toBuildCuboids) {
        this.toBuildCuboids = toBuildCuboids;
    }

    public void addCuboid(IndexEntity cuboid) {
        this.toBuildCuboids.add(cuboid);
    }

    public void setParentStorageDF(Dataset<Row> parentDF) {
        this.parentDF = parentDF;
    }
}
