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

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
public class LayoutRecDetailResponse implements Serializable {
    @JsonProperty("sqls")
    private List<String> sqlList = Lists.newArrayList();
    @JsonProperty("index_id")
    private long indexId;
    @JsonProperty("dimensions")
    private List<RecDimension> dimensions = Lists.newArrayList();
    @JsonProperty("measures")
    private List<RecMeasure> measures = Lists.newArrayList();
    @JsonProperty("computed_columns")
    private List<RecComputedColumn> computedColumns = Lists.newArrayList();

    @Getter
    @Setter
    @NoArgsConstructor
    public static class RecDimension implements Serializable {
        private NDataModel.NamedColumn dimension;
        private String dataType;
        private boolean isNew;

        public RecDimension(NDataModel.NamedColumn dimension, boolean isNew, String dataType) {
            this.dataType = dataType;
            this.dimension = dimension;
            this.isNew = isNew;
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class RecMeasure implements Serializable {
        private NDataModel.Measure measure;
        private boolean isNew;

        public RecMeasure(NDataModel.Measure measure, boolean isNew) {
            this.measure = measure;
            this.isNew = isNew;
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class RecComputedColumn implements Serializable {
        private ComputedColumnDesc cc;
        private boolean isNew;

        public RecComputedColumn(ComputedColumnDesc cc, boolean isNew) {
            this.cc = cc;
            this.isNew = isNew;
        }
    }

}
