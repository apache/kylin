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

import java.util.List;

import org.apache.kylin.common.util.ArgsTypeJsonDeserializer;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.dao.ExecutablePO;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Data;

@Data
public class OptimizeLayoutDataRequest {

    @JsonDeserialize(using = ArgsTypeJsonDeserializer.IntegerJsonDeserializer.class)
    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    @JsonProperty("yarn_queue")
    private String yarnQueue;

    @JsonProperty("project")
    private String project;

    @JsonProperty("model_optimization_setting")
    private DataOptimizationSetting modelOptimizationSetting;

    @JsonProperty("layout_optimization_settings")
    private List<LayoutDataOptimizationSetting> layoutDataOptimizationSettingList;

    @Data
    public static class LayoutDataOptimizationSetting {
        @JsonProperty("layout_id_list")
        private List<Long> layoutIdList;

        @JsonProperty("optimization_setting")
        private DataOptimizationSetting setting;
    }

    @Data
    public static class DataOptimizationSetting {

        @JsonProperty("repartition_by_columns")
        private List<String> repartitionByColumns;

        @JsonProperty("zorder_by_columns")
        private List<String> zorderByColumns;

        @JsonProperty("compaction")
        private boolean compaction = true;

        @JsonProperty("min_compaction_file_size")
        private long minCompactionFileSize;

        @JsonProperty("max_compaction_file_size")
        private long maxCompactionFileSize;

        @JsonProperty("auto_optimize")
        private boolean autoOptimize;
    }

    public static OptimizeLayoutDataRequest template;

    static {
        template = new OptimizeLayoutDataRequest();
        template.setProject("");
        DataOptimizationSetting modelSetting = new DataOptimizationSetting();
        modelSetting.setMaxCompactionFileSize(0);
        modelSetting.setMinCompactionFileSize(0);
        modelSetting.setRepartitionByColumns(Lists.newArrayList("TABLE_NAME.COLUMN_NAME"));
        modelSetting.setZorderByColumns(Lists.newArrayList("TABLE_NAME.COLUMN_NAME"));
        template.setModelOptimizationSetting(modelSetting);
        LayoutDataOptimizationSetting layoutSetting = new LayoutDataOptimizationSetting();
        DataOptimizationSetting setting = new DataOptimizationSetting();
        setting.setMaxCompactionFileSize(0);
        setting.setMinCompactionFileSize(0);
        setting.setRepartitionByColumns(Lists.newArrayList("TABLE_NAME.COLUMN_NAME"));
        setting.setZorderByColumns(Lists.newArrayList("TABLE_NAME.COLUMN_NAME"));
        layoutSetting.setLayoutIdList(Lists.newArrayList(0L));
        layoutSetting.setSetting(setting);
        template.setLayoutDataOptimizationSettingList(Lists.newArrayList(layoutSetting));
    }
}
