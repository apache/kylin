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

package io.kyligence.kap.secondstorage.management.request;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SecondStorageIndexResponse {
    @JsonProperty("is_current")
    private boolean current;

    @JsonProperty("layout_id")
    private long layoutId;

    @JsonProperty("primary_index_status")
    private SecondStorageIndexLoadStatus primaryIndexStatus;

    @JsonProperty("secondary_index_status")
    private SecondStorageIndexLoadStatus secondaryIndexStatus;

    @JsonProperty("primary_index_columns")
    private List<Column> primaryIndexColumns;

    @JsonProperty("secondary_index_columns")
    private List<Column> secondaryIndexColumns;

    @JsonProperty("primary_index_last_modified")
    private long primaryIndexLastModified;

    @JsonProperty("secondary_index_last_modified")
    private long secondaryIndexLastModified;

    @JsonProperty("is_has_data")
    private boolean hasData;

    public SecondStorageIndexResponse(long layoutId, boolean current, List<Column> primaryIndexColumns,
            List<Column> secondaryIndexColumns, long primaryIndexLastModified, long secondaryIndexLastModified) {
        this.layoutId = layoutId;
        this.current = current;
        this.primaryIndexColumns = primaryIndexColumns;
        this.secondaryIndexColumns = secondaryIndexColumns;
        this.primaryIndexLastModified = primaryIndexLastModified;
        this.secondaryIndexLastModified = secondaryIndexLastModified;
    }

    public void initIndexStatus(SecondStorageIndexLoadStatus secondaryStatus, boolean hasData) {
        this.secondaryIndexStatus = secondaryStatus;

        if (hasData) {
            this.primaryIndexStatus = SecondStorageIndexLoadStatus.ALL;
            this.hasData = true;
        } else {
            this.primaryIndexStatus = SecondStorageIndexLoadStatus.NONE;
            this.hasData = false;
        }
    }

    @Data
    @AllArgsConstructor
    public static class Column {
        @JsonProperty("name")
        String name;

        @JsonProperty("type")
        String type;
    }
}
