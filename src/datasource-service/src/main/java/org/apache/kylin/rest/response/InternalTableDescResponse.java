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

package org.apache.kylin.rest.response;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.table.InternalTableDesc;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class InternalTableDescResponse {

    @JsonProperty("uuid")
    private String uuid;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("database_name")
    private String databaseName;

    @JsonProperty("time_partition_col")
    private String timePartitionCol;

    @JsonProperty("date_partition_format")
    private String datePartitionFormat;

    @JsonProperty("hit_count")
    private long hitCount;

    @JsonProperty("row_count")
    private long rowCount;

    @JsonProperty("storage_size")
    private long storageSize;

    @JsonProperty("update_time")
    private long updateTime;

    @JsonProperty("tbl_properties")
    private Map<String, String> tblProperties;

    @JsonProperty("columns_info")
    private List<ColumnDesc> columns;

    public static InternalTableDescResponse convertToResponse(InternalTableDesc internalTableDesc,
            boolean needDetails) {
        InternalTableDescResponse response = new InternalTableDescResponse();
        response.setTableName(internalTableDesc.getName());
        response.setUuid(internalTableDesc.getUuid());
        response.setDatabaseName(internalTableDesc.getDatabase());
        response.setRowCount(internalTableDesc.getRowCount());
        response.setStorageSize(internalTableDesc.getStorageSize());
        response.setHitCount(internalTableDesc.getHitCount());

        String[] partitionColumns = internalTableDesc.getPartitionColumns();
        String partitionColumn = ArrayUtils.isNotEmpty(partitionColumns) ? partitionColumns[0] : null;
        response.setTimePartitionCol(partitionColumn);

        response.setUpdateTime(internalTableDesc.getLastModified());
        response.setDatePartitionFormat(internalTableDesc.getDatePartitionFormat());
        response.setTblProperties(internalTableDesc.getTblProperties());

        if (needDetails) {
            response.setColumns(Arrays.asList(internalTableDesc.getColumns()));
        }
        return response;
    }
}
