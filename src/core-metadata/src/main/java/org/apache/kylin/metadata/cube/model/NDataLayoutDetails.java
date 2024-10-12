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

package org.apache.kylin.metadata.cube.model;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.TreeRangeSetDeserializer;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.TreeRangeSet;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.NProjectManager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

@SuppressWarnings("serial")
@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataLayoutDetails extends RootPersistentEntity implements Serializable {

    @JsonProperty("project")
    private String project;

    @JsonProperty("dataflow")
    private String modelId;

    @JsonProperty("layout")
    private long layoutId;

    @JsonProperty("fragment_range_set")
    @JsonDeserialize(using = TreeRangeSetDeserializer.class)
    private TreeRangeSet<Long> fragmentRangeSet = TreeRangeSet.create();

    @JsonProperty("partition_columns")
    private List<String> partitionColumns;

    @JsonProperty("zorder_by_columns")
    private List<String> zorderByColumns;

    @JsonProperty("max_compaction_file_size_in_bytes")
    private long maxCompactionFileSizeInBytes;

    @JsonProperty("min_compaction_file_size_in_bytes")
    private long minCompactionFileSizeInBytes;

    @JsonProperty("compaction_after_update")
    private boolean compactionAfterUpdate = NProjectManager.getProjectConfig(project)
            .isCompactionAfterDataUpdateEnabled();

    @JsonProperty("location")
    private String location;

    @JsonProperty("num_of_files")
    private long numOfFiles;

    @JsonProperty("num_of_remove_files")
    private long numOfRemoveFiles;

    @JsonProperty("size_in_bytes")
    private long sizeInBytes;

    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonProperty("table_version")
    private long tableVersion;

    @Getter(lazy = true)
    private final String rangeFilterExpr = genRangerFilterExpr();

    @Override
    public String resourceName() {
        return modelId + "-" + layoutId;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.LAYOUT_DETAILS;
    }

    @Setter
    private KylinConfigExt config = null;

    public KylinConfig getConfig() {
        return config == null ? KylinConfig.getInstanceFromEnv() : config;
    }

    public String getLocation() {
        String result = location;
        if (StringUtils.isBlank(location)) {
            String hdfsWorkingDir = KapConfig.wrap(getConfig()).getMetadataWorkingDirectory();
            String storagePath1WithoutPrefix = String.format(Locale.ROOT, "%s/delta/%s/%s", project, modelId, layoutId);
            result = hdfsWorkingDir + storagePath1WithoutPrefix;
        }
        return result;
    }

    public String genRangerFilterExpr() {
        val dfManager = NDataflowManager.getInstance(getConfig(), project);
        val df = dfManager.getDataflow(modelId);
        if (df.getModel().isIncrementBuildOnExpertMode()) {
            PartitionDesc partitionDesc = df.getModel().getPartitionDesc();
            PartitionDesc.IPartitionConditionBuilder conditionBuilder = partitionDesc.getPartitionConditionBuilder();
            String expr = fragmentRangeSet.asRanges().stream().map(longRange -> {
                SegmentRange<Long> segmentRange = new SegmentRange.TimePartitionedSegmentRange(
                        longRange.lowerEndpoint(), longRange.upperEndpoint());
                return conditionBuilder.buildDateRangeCondition(partitionDesc, null, segmentRange).replace(
                        partitionDesc.getPartitionDateColumnRef().getBackTickExp(),
                        "`" + df.getModel().getPartitionColumnId() + "`");
            }).reduce((current, res) -> res + " or " + current).orElse("");
            return expr;
        } else
            return null;
    }

    public List<String> getPartitionColumns() {
        val dfManager = NDataflowManager.getInstance(getConfig(), project);
        val df = dfManager.getDataflow(modelId);
        if (this.partitionColumns == null || this.partitionColumns.isEmpty()) {
            String overrideConfig = df.getIndexPlan().getOverrideProps()
                    .get(IndexPlan.STORAGE_V3_MODEL_DEFAULT_PARTITION_BY_CONF_KEY);
            if (!StringUtils.isEmpty(overrideConfig)) {
                return Lists.newArrayList(overrideConfig.split(IndexPlan.STORAGE_V3_CONFIG_COLUMN_SEPARATOR));
            } else {
                return Lists.newArrayList();
            }
        } else {
            return this.partitionColumns;
        }
    }

    public List<String> getZorderByColumns() {
        val dfManager = NDataflowManager.getInstance(getConfig(), project);
        val df = dfManager.getDataflow(modelId);
        if (this.zorderByColumns == null || this.zorderByColumns.isEmpty()) {
            String overrideConfig = df.getIndexPlan().getOverrideProps()
                    .get(IndexPlan.STORAGE_V3_MODEL_DEFAULT_ZORDER_BY_CONF_KEY);
            if (!StringUtils.isEmpty(overrideConfig)) {
                return Lists.newArrayList(overrideConfig.split(IndexPlan.STORAGE_V3_CONFIG_COLUMN_SEPARATOR));
            } else {
                return Lists.newArrayList();
            }
        } else {
            return this.zorderByColumns;
        }
    }

    public long getMaxCompactionFileSizeInBytes() {
        val dfManager = NDataflowManager.getInstance(getConfig(), project);
        val df = dfManager.getDataflow(modelId);
        if (this.maxCompactionFileSizeInBytes == 0) {
            return Long.parseLong(df.getIndexPlan().getOverrideProps()
                    .getOrDefault(IndexPlan.STORAGE_V3_MODEL_DEFAULT_MAX_FILE_SIZE_CONF_KEY, "0"));
        } else {
            return this.maxCompactionFileSizeInBytes;
        }
    }

    public long getMinCompactionFileSizeInBytes() {
        val dfManager = NDataflowManager.getInstance(getConfig(), project);
        val df = dfManager.getDataflow(modelId);
        if (this.minCompactionFileSizeInBytes == 0) {
            return Long.parseLong(df.getIndexPlan().getOverrideProps()
                    .getOrDefault(IndexPlan.STORAGE_V3_MODEL_DEFAULT_MIN_FILE_SIZE_CONF_KEY, "0"));
        } else {
            return this.minCompactionFileSizeInBytes;
        }
    }

    public String getRelativeStoragePath() {
        return project + "/delta/" + getModelId() + "/" + getLayoutId();
    }
}
