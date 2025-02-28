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

import java.util.Map;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PreReloadTableResponse {

    @JsonProperty("add_column_count")
    private long addColumnCount;

    @JsonProperty("remove_column_count")
    private long removeColumnCount;

    @JsonProperty("data_type_change_column_count")
    private long dataTypeChangeColumnCount;

    @JsonProperty("broken_model_count")
    private long brokenModelCount;

    @JsonProperty("remove_measures_count")
    private long removeMeasureCount;

    @JsonProperty("remove_dimensions_count")
    private long removeDimCount;

    @JsonProperty("remove_layouts_count")
    private long removeLayoutsCount;

    @JsonProperty("add_layouts_count")
    private long addLayoutsCount;

    @JsonProperty("refresh_layouts_count")
    private long refreshLayoutsCount;

    @JsonProperty("snapshot_deleted")
    private boolean snapshotDeleted = false;

    @JsonProperty("table_comment_changed")
    private boolean tableCommentChanged = false;

    @JsonProperty("update_base_index_count")
    private int updateBaseIndexCount;

    @JsonProperty("details")
    private PreReloadTableDetails details = new PreReloadTableDetails();

    public PreReloadTableResponse() {
    }

    public PreReloadTableResponse(PreReloadTableResponse otherResponse) {
        this.addColumnCount = otherResponse.addColumnCount;
        this.removeColumnCount = otherResponse.removeColumnCount;
        this.dataTypeChangeColumnCount = otherResponse.dataTypeChangeColumnCount;
        this.brokenModelCount = otherResponse.brokenModelCount;
        this.removeMeasureCount = otherResponse.removeMeasureCount;
        this.removeDimCount = otherResponse.removeDimCount;
        this.removeLayoutsCount = otherResponse.removeLayoutsCount;
        this.addLayoutsCount = otherResponse.addLayoutsCount;
        this.refreshLayoutsCount = otherResponse.refreshLayoutsCount;
        this.snapshotDeleted = otherResponse.snapshotDeleted;
        this.tableCommentChanged = otherResponse.tableCommentChanged;
        this.details = otherResponse.details;
    }

    @Getter
    @Setter
    public class PreReloadTableDetails {
        @JsonProperty("added_columns")
        private Set<String> addedColumns = Sets.newHashSet();

        @JsonProperty("removed_columns")
        private Set<String> removedColumns = Sets.newHashSet();

        @JsonProperty("data_type_changed_columns")
        private Set<String> dataTypeChangedColumns = Sets.newHashSet();

        @JsonProperty("broken_models")
        private Set<String> brokenModels = Sets.newHashSet();

        @JsonProperty("removed_measures")
        private Set<String> removedMeasures = Sets.newHashSet();

        @JsonProperty("removed_dimensions")
        private Set<String> removedDimensions = Sets.newHashSet();

        @JsonProperty("removed_layouts")
        private Map<String, Set<Long>> removedLayouts = Maps.newHashMap();

        @JsonProperty("added_layouts")
        private Map<String, Set<Long>> addedLayouts = Maps.newHashMap();

        @JsonProperty("refreshed_layouts")
        private Map<String, Set<Long>> refreshedLayouts = Maps.newHashMap();

        PreReloadTableDetails() {
        }
    }
}
