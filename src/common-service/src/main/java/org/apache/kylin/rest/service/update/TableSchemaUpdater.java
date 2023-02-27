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

package org.apache.kylin.rest.service.update;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class TableSchemaUpdater {

    public static TableDesc dealWithMappingForTable(TableDesc other, Map<String, TableSchemaUpdateMapping> mappings) {
        TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, other.getIdentity());
        if (mapping == null) {
            return other;
        }

        TableDesc copy = new TableDesc(other, false);

        copy.setDatabase(mapping.getDatabase(other.getDatabase()));
        copy.setName(mapping.getTableName(other.getName()));
        // It will always be a new one
        copy.setLastModified(0L);

        return copy;
    }

    public static NDataModel dealWithMappingForModel(KylinConfig config, String project, NDataModel other,
            Map<String, TableSchemaUpdateMapping> mappings) {

        // Currently, model with filter condition is not supported.
        if (!Strings.isNullOrEmpty(other.getFilterCondition())) {
            throw new UnsupportedOperationException("Cannot deal with filter condition " + other.getFilterCondition());
        }

        if ((!config.isSupportUpdateComputedColumnMapping()) && (other.getComputedColumnDescs().size() != 0)) {
            throw new UnsupportedOperationException(
                    "Do not support deal with computed column " + other.getComputedColumnDescs());
        }
        NDataModel copy = NDataModelManager.getInstance(config, project).copyForWrite(other);

        // mapping for root fact table identity
        TableSchemaUpdateMapping rootMapping = getTableSchemaUpdateMapping(mappings, other.getRootFactTableName());
        if (rootMapping != null) {
            TableDesc rootFactTable = other.getRootFactTable().getTableDesc();
            copy.setRootFactTableName(
                    rootMapping.getTableIdentity(rootFactTable.getDatabase(), rootFactTable.getName()));
        }

        // mapping for join tables
        List<JoinTableDesc> joinTables = other.getJoinTables();
        List<JoinTableDesc> joinTablesCopy = new ArrayList<>(joinTables.size());
        for (int i = 0; i < joinTables.size(); i++) {
            JoinTableDesc joinTable = joinTables.get(i);
            joinTablesCopy.add(JoinTableDesc.getCopyOf(joinTable));
            String tableIdentity = joinTable.getTable();
            TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, tableIdentity);
            if (mapping != null && mapping.isTableIdentityChanged()) {
                joinTablesCopy.get(i).setTable(mapping.getTableIdentity(tableIdentity));
            }
        }
        copy.setJoinTables(joinTablesCopy);

        //mapping for computed columns
        List<ComputedColumnDesc> computedColumns = other.getComputedColumnDescs();
        List<ComputedColumnDesc> computedColumnsCopy = new ArrayList<>(computedColumns.size());
        for (int i = 0; i < computedColumns.size(); i++) {
            ComputedColumnDesc columnDesc = computedColumns.get(i);
            computedColumnsCopy.add(ComputedColumnDesc.getCopyOf(columnDesc));
            String tableIdentity = columnDesc.getTableIdentity();
            TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, tableIdentity);
            if (mapping != null && mapping.isTableIdentityChanged()) {
                computedColumnsCopy.get(i).setTableIdentity(mapping.getTableIdentity(tableIdentity));
            }
        }
        copy.setComputedColumnDescs(computedColumnsCopy);

        return copy;
    }

    public static NDataflow dealWithMappingForDataFlow(KylinConfig config, String project, NDataflow other, Map<String, TableSchemaUpdateMapping> mappings) {

        NDataflow copy = NDataflowManager.getInstance(config, project).copy(other);
        copy.setLastModified(other.getLastModified());

        // mapping for segments
        if (other.getSegments() != null && !other.getSegments().isEmpty()) {
            Segments<NDataSegment> segmentsCopy = new Segments<>();
            for (NDataSegment segment : other.getSegments()) {
                NDataSegment segmentCopy = new NDataSegment(segment);
                segmentCopy.setDataflow(copy);
                // mapping for snapshot
                Map<String, String> snapshotCopy =
                        replaceTableIdentityForTableSnapshots(segment.getSnapshots(), mappings);
                // mapping for column source bytes
                Map<String, Long> columnSourceBytesCopy =
                        replaceColumnIdentityForColumnSourceBytes(segment.getColumnSourceBytes(), mappings);

                segmentCopy.setSnapshots(snapshotCopy);
                segmentCopy.setColumnSourceBytes(columnSourceBytesCopy);

                segmentsCopy.add(segmentCopy);
            }
            copy.setSegments(segmentsCopy);
        }
        return copy;
    }

    private static Map<String, String> replaceTableIdentityForTableSnapshots(Map<String, String> snapshots,
            Map<String, TableSchemaUpdateMapping> mappings) {
        Map<String, String> snapshotsCopy = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (String tableIdentity : snapshots.keySet()) {
            String resPath = snapshots.get(tableIdentity);
            TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, tableIdentity);
            if (mapping != null && mapping.isTableIdentityChanged()) {
                tableIdentity = mapping.getTableIdentity(tableIdentity);
            }
            snapshotsCopy.put(tableIdentity, resPath);
        }
        return snapshotsCopy;
    }

    private static Map<String, Long> replaceColumnIdentityForColumnSourceBytes(Map<String, Long> columnSourceBytes,
            Map<String, TableSchemaUpdateMapping> mappings) {
        Map<String, Long> copy = Maps.newHashMapWithExpectedSize(columnSourceBytes.size());
        for (String columnIdentity : columnSourceBytes.keySet()) {
            Long bytes = columnSourceBytes.get(columnIdentity);
            String tableIdentity = columnIdentity.substring(0, columnIdentity.lastIndexOf("."));
            String columnName = columnIdentity.substring(columnIdentity.lastIndexOf("."));
            TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, tableIdentity);
            if (mapping != null && mapping.isTableIdentityChanged()) {
                tableIdentity = mapping.getTableIdentity(tableIdentity);
                columnIdentity = tableIdentity + columnName;
            }
            copy.put(columnIdentity, bytes);
        }
        return copy;
    }

    public static TableSchemaUpdateMapping getTableSchemaUpdateMapping(Map<String, TableSchemaUpdateMapping> mappings,
                                                                       String key) {
        return mappings.get(key.toUpperCase(Locale.ROOT));
    }
}
