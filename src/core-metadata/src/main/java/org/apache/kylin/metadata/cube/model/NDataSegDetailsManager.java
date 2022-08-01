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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import lombok.val;

/**
 * Package private. Not intended for public use.
 * <p>
 * Public use goes through NDataflowManager.
 */
class NDataSegDetailsManager {
    private static final Serializer<NDataSegDetails> DATA_SEG_LAYOUT_INSTANCES_SERIALIZER = new JsonSerializer<>(
            NDataSegDetails.class);

    private static final Logger logger = LoggerFactory.getLogger(NDataSegDetailsManager.class);

    public static NDataSegDetailsManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataSegDetailsManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataSegDetailsManager newInstance(KylinConfig config, String project) {
        return new NDataSegDetailsManager(config, project);
    }

    // ============================================================================

    private KylinConfig kylinConfig;
    private String project;

    private NDataSegDetailsManager(KylinConfig config, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NDataSegDetailsManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(config), project);
        this.kylinConfig = config;
        this.project = project;
    }

    public KylinConfig getConfig() {
        return kylinConfig;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    NDataSegDetails getForSegment(NDataflow df, String segId) {
        NDataSegDetails instances = getStore().getResource(getResourcePathForSegment(df.getUuid(), segId),
                DATA_SEG_LAYOUT_INSTANCES_SERIALIZER);
        if (instances != null) {
            instances.setConfig(df.getConfig());
            instances.setProject(project);
        }
        return instances;
    }

    NDataSegDetails getForSegment(NDataSegment segment) {
        return getForSegment(segment.getDataflow(), segment.getId());
    }

    void updateDataflow(NDataflow df, NDataflowUpdate update) {

        // figure out all impacted segments
        Set<String> allSegIds = new TreeSet<>();
        Map<String, List<NDataLayout>> toUpsert = new TreeMap<>();
        Map<String, List<NDataLayout>> toRemove = new TreeMap<>();
        if (update.getToAddOrUpdateLayouts() != null) {
            Arrays.stream(update.getToAddOrUpdateLayouts()).forEach(c -> {
                val segId = c.getSegDetails().getUuid();
                allSegIds.add(segId);
                List<NDataLayout> list = toUpsert.computeIfAbsent(segId, k -> new ArrayList<>());
                list.add(c);
            });
        }
        if (update.getToRemoveLayouts() != null) {
            Arrays.stream(update.getToRemoveLayouts()).forEach(c -> {
                val segId = c.getSegDetails().getUuid();
                allSegIds.add(segId);
                List<NDataLayout> list = toRemove.computeIfAbsent(segId, k -> new ArrayList<>());
                list.add(c);
            });
        }
        if (update.getToAddSegs() != null) {
            Arrays.stream(update.getToAddSegs()).map(NDataSegment::getId).forEach(allSegIds::add);
        }

        // upsert for each segment
        for (String segId : allSegIds) {
            NDataSegDetails details = getForSegment(df, segId);
            if (details == null)
                details = NDataSegDetails.newSegDetails(df, segId);

            if (toUpsert.containsKey(segId)) {
                for (NDataLayout c : toUpsert.get(segId)) {
                    c.setSegDetails(details);
                    details.addLayout(c);
                }
            }
            if (toRemove.containsKey(segId)) {
                for (NDataLayout c : toRemove.get(segId)) {
                    details.removeLayout(c);
                }
            }

            upsertForSegmentQuietly(details);
        }

        if (update.getToRemoveSegs() != null) {
            for (NDataSegment seg : update.getToRemoveSegs()) {
                removeForSegmentQuietly(df, seg.getId());
            }
        }
    }

    private NDataSegDetails upsertForSegmentQuietly(NDataSegDetails details) {
        try {
            return upsertForSegment(details);
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Failed to insert/update NDataSegDetails for segment "
                    + details.getDataflowId() + "." + details.getUuid(), e);
        } catch (Exception e) {
            logger.error("Failed to insert/update NDataSegDetails for segment {}",
                    details.getDataflowId() + "." + details.getUuid(), e);
            return null;
        }
    }

    NDataSegDetails upsertForSegment(NDataSegDetails details) {
        Preconditions.checkNotNull(details, "NDataSegDetails cannot be null.");

        getStore().checkAndPutResource(details.getResourcePath(), details, DATA_SEG_LAYOUT_INSTANCES_SERIALIZER);
        return details;
    }

    private void removeForSegmentQuietly(NDataflow df, String segId) {
        try {
            removeForSegment(df, segId);
        } catch (Exception e) {
            logger.error("Failed to remove NDataSegDetails for segment {}", df + "." + segId, e);
        }
    }

    /**
     * delete the segment from the restore.
     *
     * @param df
     * @param segId
     */
    void removeForSegment(NDataflow df, String segId) {
        if (!getStore().exists(getResourcePathForSegment(df.getUuid(), segId))) {
            return;
        }

        getStore().deleteResource(getResourcePathForSegment(df.getUuid(), segId));
    }

    void removeDetails(NDataflow df) {
        val toBeRemoved = getStore().listResourcesRecursively(getResourcePathForDetails(df.getId()));
        if (CollectionUtils.isNotEmpty(toBeRemoved)) {
            toBeRemoved.forEach(path -> getStore().deleteResource(path));
        }
    }

    private String getResourcePathForSegment(String dfId, String segId) {
        return getResourcePathForDetails(dfId) + "/" + segId + MetadataConstants.FILE_SURFIX;
    }

    private String getResourcePathForDetails(String dfId) {
        return "/" + project + NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT + "/" + dfId;
    }
}
