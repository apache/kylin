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

package org.apache.kylin.source.kafka.model;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;

public class StreamCubeFactTableDesc implements IJoinedFlatTableDesc, Serializable {
    protected final CubeDesc cubeDesc;
    protected final CubeSegment cubeSegment;
    protected final IJoinedFlatTableDesc flatTableDesc;
    protected String tableName;

    public StreamCubeFactTableDesc(CubeDesc cubeDesc, CubeSegment cubeSegment, IJoinedFlatTableDesc flatTableDesc) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.flatTableDesc = flatTableDesc;
        this.tableName = makeTableName(cubeDesc, cubeSegment);
    }

    public IJoinedFlatTableDesc getFlatTableDesc() {
        return flatTableDesc;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public DataModelDesc getDataModel() {
        return cubeDesc.getModel();
    }

    @Override
    /**
     * Get all columns that need be appeared on the fact table
     */
    public List<TblColRef> getAllColumns() {
        TableRef rootFactTable = getDataModel().getRootFactTable();

        // Get the fact column from external flat table. Kylin may already optimized to skip the join keys.
        // But for stream case, the join key will be used in next step, so we need to keep them.
        List<TblColRef> factColumns = flatTableDesc.getFactColumns();
        // Add back columns which belongs to root fact table in join relation, but was absent
        for (JoinTableDesc joinTableDesc : getDataModel().getJoinTables()) {
            JoinDesc join = joinTableDesc.getJoin();
            for (TblColRef colRef : join.getForeignKeyColumns()) {
                if (colRef.getTableRef().equals(rootFactTable)) {
                    if (!factColumns.contains(colRef)) {
                        factColumns.add(colRef);
                    }
                }
            }
        }
        return factColumns;
    }

    @Override
    public List<TblColRef> getFactColumns() {
        return getAllColumns();
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        return 0;
    }

    @Override
    public SegmentRange getSegRange() {
        return null;
    }

    @Override
    public TblColRef getDistributedBy() {
        return null;
    }

    @Override
    public TblColRef getClusterBy() {
        return null;
    }

    @Override
    public ISegment getSegment() {
        return null;
    }

    @Override
    public boolean useAlias() {
        return false;
    }

    /**
     * Make a mock table name for this stream data set.
     * @param cubeDesc
     * @param cubeSegment
     * @return
     */
    protected String makeTableName(CubeDesc cubeDesc, CubeSegment cubeSegment) {
        return cubeDesc.getConfig().getHiveIntermediateTablePrefix() + cubeDesc.getName().toLowerCase(Locale.ROOT) + "_"
                + cubeSegment.getUuid().replaceAll("-", "_") + "_fact";
    }
}
