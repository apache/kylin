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

import static org.apache.kylin.metadata.model.util.MultiPartitionUtil.isSameValue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MultiPartitionDesc implements Serializable {

    @JsonProperty("columns")
    private LinkedList<String> columns;

    @JsonProperty("partitions")
    private List<PartitionInfo> partitions = new ArrayList<>();

    private String partitionConditionBuilderClz = MultiPartitionDesc.DefaultMultiPartitionConditionBuilder.class
            .getName();

    @JsonProperty("max_partition_id")
    @Setter
    private long maxPartitionID = -1;

    private LinkedList<TblColRef> columnRefs = Lists.newLinkedList();
    private Map<Long, PartitionInfo> partitionInfoMap = Maps.newHashMap();

    private PartitionDesc.IPartitionConditionBuilder partitionConditionBuilder;

    public void init(NDataModel model) {
        if (CollectionUtils.isEmpty(columns))
            return;
        for (String columnDesc : columns) {
            columnRefs.add(model.findColumn(columnDesc));
        }
        initPartitionMap();

        partitionConditionBuilder = (PartitionDesc.IPartitionConditionBuilder) ClassUtil
                .newInstance(partitionConditionBuilderClz);
    }

    public void initPartitionMap() {
        partitionInfoMap = partitions.stream().collect(Collectors.toMap(PartitionInfo::getId, partition -> partition));
    }

    public void removePartitionValue(List<Long> toBeDeletedPartIds) {
        if (CollectionUtils.isEmpty(toBeDeletedPartIds)) {
            return;
        }
        Set<Long> toBeDeletedPartIdSet = new HashSet<>(toBeDeletedPartIds);
        partitions.removeIf(partitionInfo -> toBeDeletedPartIdSet.contains(partitionInfo.getId()));
    }

    public PartitionInfo getPartitionByValue(String[] newValue) {
        Preconditions.checkState(newValue.length == columns.size());
        for (int i = 0; i < partitions.size(); i++) {
            PartitionInfo partition = partitions.get(i);
            if (isSameValue(partition.getValues(), newValue)) {
                return partition;
            }
        }
        return null;
    }

    public Set<Long> getPartitionIdsByValues(List<String[]> subPartitionValues) {
        Set<Long> partitionIds = Sets.newHashSet();
        if (subPartitionValues == null) {
            return partitionIds;
        }
        subPartitionValues.forEach(partition -> {
            PartitionInfo partitionInfo = getPartitionByValue(partition);
            if (partitionInfo != null) {
                partitionIds.add(partitionInfo.getId());
            }
        });
        return partitionIds;
    }

    public MultiPartitionDesc.PartitionInfo getPartitionInfo(long id) {
        return partitionInfoMap.get(id);
    }

    public List<String[]> getPartitionValuesById(List<Long> partitionId) {
        if (MapUtils.isEmpty(partitionInfoMap)) {
            initPartitionMap();
        }
        List<String[]> partValues = Lists.newArrayList();
        partitionId.forEach(id -> {
            Preconditions.checkNotNull(partitionInfoMap.get(id));
            partValues.add(partitionInfoMap.get(id).getValues());
        });
        return partValues;
    }

    public PartitionDesc.IPartitionConditionBuilder getPartitionConditionBuilder() {
        return partitionConditionBuilder;
    }

    public static class DefaultMultiPartitionConditionBuilder
            implements PartitionDesc.IPartitionConditionBuilder, Serializable {

        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange) {
            val builder = partDesc.getPartitionConditionBuilder();
            return builder.buildDateRangeCondition(partDesc, seg, segRange);
        }

        @Override
        public String buildMultiPartitionCondition(final PartitionDesc partDesc, final MultiPartitionDesc multiPartDesc,
                final LinkedList<Long> partitionIds, final ISegment seg, final SegmentRange segRange) {
            String timeRange;
            if (PartitionDesc.isEmptyPartitionDesc(partDesc) || Objects.isNull(segRange) || segRange.isInfinite()) {
                timeRange = null;
            } else {
                timeRange = buildDateRangeCondition(partDesc, seg, segRange);
            }

            if (Objects.isNull(timeRange) && CollectionUtils.isEmpty(partitionIds)) {
                return null;
            }

            StringBuilder builder = new StringBuilder();
            if (Objects.nonNull(timeRange)) {
                builder.append(timeRange);
            }

            String mlpCondition = buildMLPCondition(multiPartDesc, seg, partitionIds);
            if (Objects.nonNull(mlpCondition)) {
                builder.append(" and ").append(mlpCondition);
            }

            return builder.toString();
        }

        public static String buildMLPCondition(final MultiPartitionDesc multiPartDesc, final ISegment seg,
                List<Long> partitionIDs) {
            if (CollectionUtils.isEmpty(partitionIDs)) {
                return null;
            }
            List<TblColRef> columnRefs = multiPartDesc.getColumnRefs();
            List<String[]> values = partitionIDs.stream().map(multiPartDesc::getPartitionInfo)
                    .map(PartitionInfo::getValues).collect(Collectors.toList());

            List<String> conditions = Lists.newArrayList();
            for (int i = 0; i < columnRefs.size(); i++) {
                final int x = i;
                String item = columnRefs.get(x).getBackTickExp() + " in (" + //
                        values.stream().map(a -> generateFormattedValue(columnRefs.get(x).getType(), a[x]))
                                .collect(Collectors.joining(", "))
                        + ")";
                conditions.add(item);
            }

            return String.join(" and ", conditions);
        }
    }

    public static String generateFormattedValue(DataType dataType, String value) {
        if (dataType.isBoolean()) {
            return String.format(Locale.ROOT, "cast('%s' as boolean)", value);
        }
        return String.format(Locale.ROOT, "'%s'", value);

    }

    public MultiPartitionDesc(LinkedList<String> columns) {
        this.columns = columns;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    public static class PartitionInfo implements Serializable {
        private long id;
        @EqualsAndHashCode.Include
        private String[] values;
    }
}
