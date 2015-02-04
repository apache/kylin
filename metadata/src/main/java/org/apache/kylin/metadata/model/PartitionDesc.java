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

package org.apache.kylin.metadata.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.util.StringSplitter;

/**
 * @author xduo
 * 
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class PartitionDesc {

    public static enum PartitionType {
        APPEND, //
        UPDATE_INSERT // not used since 0.7.1
    }

    @JsonProperty("partition_date_column")
    private String partitionDateColumn;
    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;
    @JsonProperty("partition_type")
    private PartitionType partitionType = PartitionType.APPEND;

    private TblColRef partitionDateColumnRef;

    public void init(Map<String, Map<String, TblColRef>> columnMap) {
        if (null != partitionDateColumn) {
            partitionDateColumn = partitionDateColumn.toUpperCase();

            String[] columns = StringSplitter.split(partitionDateColumn, ".");

            if (null != columns && columns.length == 3) {
                String tableName = columns[0].toUpperCase() + "." + columns[1].toUpperCase();
                Map<String, TblColRef> cols = columnMap.get(tableName);
                if (cols != null) {
                    partitionDateColumnRef = cols.get(columns[2].toUpperCase());
                } else {
                    throw new IllegalStateException("The table '" + tableName + "' provided in 'partition_date_column' doesn't exist.");
                }
            } else {
                throw new IllegalStateException("The 'partition_date_column' format is invalid: " + partitionDateColumn + ", it should be {db}.{table}.{column}.");
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

    public PartitionType getCubePartitionType() {
        return partitionType;
    }

    public void setCubePartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public TblColRef getPartitionDateColumnRef() {
        return partitionDateColumnRef;
    }

}
