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

package org.apache.kylin.rest.request;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.insensitive.ModelInsensitiveRequest;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import org.apache.kylin.rest.response.LayoutRecDetailResponse;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.util.SCD2SimplificationConvertUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Lists;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
public class ModelRequest extends NDataModel implements ModelInsensitiveRequest {

    @JsonProperty("project")
    private String project;

    @JsonProperty("start")
    private String start;

    @JsonProperty("end")
    private String end;

    @JsonProperty("simplified_measures")
    private List<SimplifiedMeasure> simplifiedMeasures = Lists.newArrayList();

    @JsonProperty("simplified_dimensions")
    private List<NamedColumn> simplifiedDimensions = Lists.newArrayList();

    // non-dimension columns, used for sync column alias
    // if not present, use original column name
    @JsonProperty("other_columns")
    private List<NamedColumn> otherColumns = Lists.newArrayList();

    @JsonProperty("rec_items")
    private List<LayoutRecDetailResponse> recItems = Lists.newArrayList();

    @JsonProperty("index_plan")
    private IndexPlan indexPlan;

    @JsonProperty("save_only")
    private boolean saveOnly = false;

    @JsonProperty("with_segment")
    private boolean withEmptySegment = true;

    @JsonProperty("with_model_online")
    private boolean withModelOnline = false;

    @JsonProperty("with_base_index")
    private boolean withBaseIndex = false;

    @JsonProperty("with_second_storage")
    private boolean withSecondStorage = false;

    private List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs;

    @JsonProperty("join_tables")
    public void setSimplifiedJoinTableDescs(List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs) {
        this.simplifiedJoinTableDescs = simplifiedJoinTableDescs;
        this.setJoinTables(SCD2SimplificationConvertUtil.convertSimplified2JoinTables(simplifiedJoinTableDescs));
    }

    @JsonProperty("join_tables")
    public List<SimplifiedJoinTableDesc> getSimplifiedJoinTableDescs() {
        return simplifiedJoinTableDescs;
    }

    @JsonSetter("dimensions")
    public void setDimensions(List<NamedColumn> dimensions) {
        setSimplifiedDimensions(dimensions);
    }

    @JsonSetter("all_measures")
    public void setMeasures(List<Measure> inputMeasures) {
        List<Measure> measures = inputMeasures != null ? inputMeasures : Lists.newArrayList();
        List<SimplifiedMeasure> simpleMeasureList = Lists.newArrayList();
        for (NDataModel.Measure measure : measures) {
            SimplifiedMeasure simplifiedMeasure = SimplifiedMeasure.fromMeasure(measure);
            simpleMeasureList.add(simplifiedMeasure);
        }
        setAllMeasures(measures);
        setSimplifiedMeasures(simpleMeasureList);
    }

    private transient BiFunction<TableDesc, Boolean, Collection<ColumnDesc>> columnsFetcher = TableRef::filterColumns;

    public ModelRequest() {
        super();
    }

    public ModelRequest(NDataModel dataModel) {
        super(dataModel);
        this.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(dataModel.getJoinTables()));
    }

}
