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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SnapshotTableDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TableSchemaUpdater {

    public static TableDesc dealWithMappingForTable(TableDesc other, Map<String, TableSchemaUpdateMapping> mappings) {
        TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, other.getIdentity());
        if (mapping == null) {
            return other;
        }

        TableDesc copy = new TableDesc(other);

        copy.setDatabase(mapping.getDatabase(other.getDatabase()));

        copy.setName(mapping.getTableName(other.getName()));

        // It will always be a new one
        copy.setLastModified(0L);

        return copy;
    }

    // the input data model should be initialized, then table names & col names will be normalized
    public static DataModelDesc dealWithMappingForModel(DataModelDesc other,
            Map<String, TableSchemaUpdateMapping> mappings) {
        // For filter condition, not support
        if (!Strings.isNullOrEmpty(other.getFilterCondition())) {
            throw new UnsupportedOperationException("Cannot deal with filter condition " + other.getFilterCondition());
        }

        DataModelDesc copy = DataModelDesc.getCopyOf(other);
        copy.setLastModified(other.getLastModified());

        // mapping for root fact table identity
        TableSchemaUpdateMapping rootMapping = getTableSchemaUpdateMapping(mappings, other.getRootFactTableName());
        if (rootMapping != null) {
            TableDesc rootFactTable = other.getRootFactTable().getTableDesc();
            copy.setRootFactTableName(
                    rootMapping.getTableIdentity(rootFactTable.getDatabase(), rootFactTable.getName()));
        }

        // mapping for joins
        JoinTableDesc[] joinTables = other.getJoinTables();
        JoinTableDesc[] joinTablesCopy = new JoinTableDesc[joinTables.length];
        for (int i = 0; i < joinTables.length; i++) {
            JoinTableDesc joinTable = joinTables[i];
            joinTablesCopy[i] = JoinTableDesc.getCopyOf(joinTable);
            String tableIdentity = joinTable.getTable();
            TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, tableIdentity);
            if (mapping != null && mapping.isTableIdentityChanged()) {
                joinTablesCopy[i].setTable(mapping.getTableIdentity(tableIdentity));
            }
        }
        copy.setJoinTables(joinTablesCopy);

        // mapping for partition columns
        PartitionDesc partDesc = other.getPartitionDesc();
        PartitionDesc partCopy = PartitionDesc.getCopyOf(partDesc);
        if (partDesc.getPartitionDateColumnRef() != null) {
            partCopy.setPartitionDateColumn(
                    replacePartitionCol(partDesc.getPartitionDateColumnRef().getCanonicalName(), mappings));
        }
        if (partDesc.getPartitionTimeColumnRef() != null) {
            partCopy.setPartitionTimeColumn(
                    replacePartitionCol(partDesc.getPartitionTimeColumnRef().getCanonicalName(), mappings));
        }
        copy.setPartitionDesc(partCopy);

        return copy;
    }

    public static CubeDesc dealWithMappingForCubeDesc(CubeDesc other, Map<String, TableSchemaUpdateMapping> mappings) {
        CubeDesc copy = CubeDesc.getCopyOf(other);
        copy.setLastModified(other.getLastModified());

        // mapping for cube-level snapshot tables
        if (other.getSnapshotTableDescList() != null && !other.getSnapshotTableDescList().isEmpty()) {
            List<SnapshotTableDesc> snapshotTableDescListCopy = Lists
                    .newArrayListWithExpectedSize(other.getSnapshotTableDescList().size());
            for (SnapshotTableDesc snapshotDesc : other.getSnapshotTableDescList()) {
                TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, snapshotDesc.getTableName());
                if (mapping != null && mapping.isTableIdentityChanged()) {
                    snapshotDesc = SnapshotTableDesc.getCopyOf(snapshotDesc);
                    snapshotDesc.setTableName(mapping.getTableIdentity(snapshotDesc.getTableName()));
                }
                snapshotTableDescListCopy.add(snapshotDesc);
            }
            copy.setSnapshotTableDescList(snapshotTableDescListCopy);
        }

        return copy;
    }

    public static CubeInstance dealWithMappingForCube(CubeInstance other,
            Map<String, TableSchemaUpdateMapping> mappings) {
        CubeInstance copy = CubeInstance.getCopyOf(other);
        copy.setLastModified(other.getLastModified());

        // mapping for cube-level snapshot tables
        if (other.getSnapshots() != null && !other.getSnapshots().isEmpty()) {
            Map<String, String> snapshotsCopy = replaceTableIdentityForTableSnapshots(other.getSnapshots(), mappings);
            copy.resetSnapshots();
            copy.getSnapshots().putAll(snapshotsCopy);
        }

        // mapping for segment-level snapshot tables
        if (other.getSegments() != null && !other.getSegments().isEmpty()) {
            Segments<CubeSegment> segmentsCopy = new Segments<>();
            for (CubeSegment segment : other.getSegments()) {
                CubeSegment segmentCopy = CubeSegment.getCopyOf(segment);
                segmentCopy.setCubeInstance(copy);
                Map<String, String> snapshotsCopy = replaceTableIdentityForTableSnapshots(segment.getSnapshots(),
                        mappings);
                segmentCopy.resetSnapshots();
                segmentCopy.getSnapshots().putAll(snapshotsCopy);
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

    private static String replacePartitionCol(String partCol, Map<String, TableSchemaUpdateMapping> mappings) {
        int cut = partCol.lastIndexOf('.');
        if (cut < 0) {
            return partCol;
        }
        String partTableIdentity = partCol.substring(0, cut);
        TableSchemaUpdateMapping mapping = getTableSchemaUpdateMapping(mappings, partTableIdentity);
        if (mapping != null) {
            return mapping.getTableIdentity(partTableIdentity) + "." + partCol.substring(cut + 1);
        }
        return partCol;
    }

    public static TableSchemaUpdateMapping getTableSchemaUpdateMapping(Map<String, TableSchemaUpdateMapping> mappings,
            String key) {
        return mappings.get(key.toUpperCase(Locale.ROOT));
    }
}
