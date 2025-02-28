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

package org.apache.kylin.utils;

import static org.apache.kylin.common.constant.Constants.ASYNC;
import static org.apache.kylin.common.constant.Constants.CACHE_MODEL_COMMAND;
import static org.apache.kylin.common.constant.Constants.CACHE_TABLE_COMMAND;
import static org.apache.kylin.common.constant.Constants.FILTER_COMMAND;
import static org.apache.kylin.common.constant.Constants.START;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetailsManager;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.table.InternalTableManager;

import lombok.val;
import lombok.var;
import lombok.experimental.UtilityClass;

@UtilityClass
public class GlutenCacheUtils {

    public static String generateCacheTableCommand(KylinConfig config, String project, String table, String start,
            List<String> columns, boolean isAsync) {
        val internalTableManager = InternalTableManager.getInstance(config, project);
        val internalTable = internalTableManager.getInternalTableDesc(table);
        if (Objects.isNull(internalTable)) {
            throw new KylinRuntimeException(String.format(Locale.ROOT, "InternalTable [%s] not exist", table));
        }
        val location = internalTable.generateInternalTableLocation();
        val tablePartition = internalTable.getTablePartition();
        var filterCommand = StringUtils.EMPTY;
        if (Objects.nonNull(tablePartition) && StringUtils.isNotBlank(start)) {
            val partitionColumn = tablePartition.getPartitionColumns()[0];
            val datePartitionFormat = tablePartition.getDatePartitionFormat();
            val formatStart = DateFormat.formatToTimeStr(Long.parseLong(start), datePartitionFormat);
            filterCommand = String.format(Locale.ROOT, FILTER_COMMAND, partitionColumn, formatStart);
        }
        var selectColumns = START;
        if (CollectionUtils.isNotEmpty(columns)) {
            Set<String> internalTableColumns = Arrays.stream(internalTable.getColumns()).map(ColumnDesc::getName)
                    .collect(Collectors.toSet());
            val notExistColumns = Sets.difference(Sets.newHashSet(columns), internalTableColumns);
            if (CollectionUtils.isNotEmpty(notExistColumns)) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, "InternalTable[%s] column[%s] not exist", table, notExistColumns));
            }
            selectColumns = String.join(",", columns);
        }
        val async = isAsync ? ASYNC : StringUtils.EMPTY;
        return String.format(Locale.ROOT, CACHE_TABLE_COMMAND, async, selectColumns, location, filterCommand);
    }

    public static Set<String> generateModelCacheCommands(KylinConfig config, String project, String modelId,
            List<String> targetSegments) {
        val modelManager = NDataModelManager.getInstance(config, project);
        val dataModelDesc = modelManager.getDataModelDesc(modelId);
        var cachePaths = Sets.<String> newHashSet();
        val dfMgr = NDataflowManager.getInstance(config, project);
        val dataflow = dfMgr.getDataflow(modelId);
        val segments = dataflow.getSegments(Sets.newHashSet(targetSegments));
        switch (dataModelDesc.getStorageType()) {
        case V1:
            val segmentPath = segments.stream().map(seg -> dataflow.getSegmentHdfsPath(seg.getId()))
                    .collect(Collectors.toSet());
            cachePaths.addAll(segmentPath);
            break;
        case DELTA:
            val layoutDetailsManager = NDataLayoutDetailsManager.getInstance(config, project);
            val indexLocations = segments.stream().map(NDataSegment::getLayoutIds).flatMap(Collection::stream)
                    .collect(Collectors.toSet()).stream()
                    .map(id -> layoutDetailsManager.getNDataLayoutDetails(modelId, id).getLocation())
                    .collect(Collectors.toSet());
            cachePaths.addAll(indexLocations);
            break;
        default:
            throw new KylinRuntimeException(String.format(Locale.ROOT,
                    "Model StorageType[%s] not support load gluten cache", dataModelDesc.getStorageType()));
        }
        return cachePaths.stream().map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, StringUtils.EMPTY, path))
                .collect(Collectors.toSet());
    }

    public static Set<String> generateModelCacheCommandsByIndexes(KylinConfig config, String project, String modelId,
            List<Long> indexes) {
        val modelManager = NDataModelManager.getInstance(config, project);
        val dataModelDesc = modelManager.getDataModelDesc(modelId);
        var cachePaths = Sets.<String> newHashSet();
        val dfMgr = NDataflowManager.getInstance(config, project);
        val df = dfMgr.getDataflow(modelId);
        val cacheIndexes = df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .filter(id -> CollectionUtils.isEmpty(indexes) || indexes.contains(id)).collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(indexes) && indexes.size() != cacheIndexes.size()) {
            val notExistIndexes = Sets.difference(Sets.newHashSet(indexes), cacheIndexes);
            throw new KylinRuntimeException(
                    String.format(Locale.ROOT, "Model[%s] indexes[%s] not exist", modelId, notExistIndexes));
        }
        if (Objects.requireNonNull(dataModelDesc.getStorageType()) == NDataModel.DataStorageType.DELTA) {
            val layoutDetailsManager = NDataLayoutDetailsManager.getInstance(config, project);
            val indexLocations = cacheIndexes.stream()
                    .map(id -> layoutDetailsManager.getNDataLayoutDetails(modelId, id).getLocation())
                    .collect(Collectors.toSet());
            cachePaths.addAll(indexLocations);
        } else {
            throw new KylinRuntimeException(String.format(Locale.ROOT,
                    "Model StorageType[%s] not support load gluten cache by indexes", dataModelDesc.getStorageType()));
        }
        return cachePaths.stream().map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, ASYNC, path))
                .collect(Collectors.toSet());
    }

    public static Set<String> generateModelCacheCommandsBySegments(KylinConfig config, String project, String modelId,
            Segments<NDataSegment> segments) {
        val modelManager = NDataModelManager.getInstance(config, project);
        val dataModelDesc = modelManager.getDataModelDesc(modelId);
        val dfMgr = NDataflowManager.getInstance(config, project);
        val dataflow = dfMgr.getDataflow(modelId);
        var cachePaths = Sets.<String> newHashSet();
        if (Objects.requireNonNull(dataModelDesc.getStorageType()) == NDataModel.DataStorageType.V1) {
            val segmentPath = segments.stream().map(seg -> dataflow.getSegmentHdfsPath(seg.getId()))
                    .collect(Collectors.toSet());
            cachePaths.addAll(segmentPath);
        } else {
            throw new KylinRuntimeException(String.format(Locale.ROOT,
                    "Model StorageType[%s] not support load gluten cache by segments", dataModelDesc.getStorageType()));
        }
        return cachePaths.stream().map(path -> String.format(Locale.ROOT, CACHE_MODEL_COMMAND, ASYNC, path))
                .collect(Collectors.toSet());
    }
}
