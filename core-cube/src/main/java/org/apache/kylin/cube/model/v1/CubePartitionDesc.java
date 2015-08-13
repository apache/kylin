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

package org.apache.kylin.cube.model.v1;

import java.util.Map;

import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author xduo
 * 
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubePartitionDesc {

    public static enum CubePartitionType {
        APPEND, UPDATE_INSERT
    }

    @JsonProperty("partition_date_column")
    private String partitionDateColumn;
    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;
    @JsonProperty("cube_partition_type")
    private CubePartitionType cubePartitionType = CubePartitionType.APPEND;

    private TblColRef partitionDateColumnRef;

    public void init(Map<String, Map<String, TblColRef>> columnMap) {
        if (null != partitionDateColumn) {
            String[] columns = partitionDateColumn.split("\\.");

            if (null != columns && columns.length == 2) {
                Map<String, TblColRef> cols = columnMap.get(columns[0].toUpperCase());
                if (cols != null)
                    partitionDateColumnRef = cols.get(columns[1].toUpperCase());

            }
        }
    }

    public boolean isPartitioned() {
        return partitionDateColumnRef != null;
    }

    public String getPartitionDateColumn() {
        return partitionDateColumn;
    }

    public void setPartitionDateColumn(String partitionDateColumn) {
        this.partitionDateColumn = partitionDateColumn;
    }

    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public CubePartitionType getCubePartitionType() {
        return cubePartitionType;
    }

    public void setCubePartitionType(CubePartitionType cubePartitionType) {
        this.cubePartitionType = cubePartitionType;
    }

    public TblColRef getPartitionDateColumnRef() {
        return partitionDateColumnRef;
    }

}
