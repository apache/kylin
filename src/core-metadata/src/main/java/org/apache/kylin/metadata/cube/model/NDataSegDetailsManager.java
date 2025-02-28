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

import static org.apache.kylin.common.persistence.MetadataType.LAYOUT;

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
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private ResourceStore resourceStore;

    private NDataSegDetailsManager(KylinConfig config, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NDataSegDetailsManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(config), project);
        this.kylinConfig = config;
        this.project = project;
        this.resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    public KylinConfig getConfig() {
        return kylinConfig;
    }

    private ResourceStore getStore() {
        return resourceStore;
    }

    private NDataSegDetails initEntity(NDataSegDetails dataSegDetails, String resourceName) {
        dataSegDetails.setProject(project);
        return dataSegDetails;
    }

    public NDataSegDetails copyForWrite(NDataSegDetails details) {
        return CachedCrudAssist.copyForWrite(details, DATA_SEG_LAYOUT_INSTANCES_SERIALIZER,  this::initEntity, resourceStore);
    }
    
    NDataSegDetails getForSegment(NDataflow df, String segId) {
        NDataSegDetails instances = getStore().getResource(MetadataType.mergeKeyWithType(segId, LAYOUT),
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
            NDataSegDetails copy = copyForWrite(details);

            if (toUpsert.containsKey(segId)) {
                for (NDataLayout c : toUpsert.get(segId)) {
                    c.setSegDetails(copy);
                    copy.addLayout(c);
                }
            }
            if (toRemove.containsKey(segId)) {
                for (NDataLayout c : toRemove.get(segId)) {
                    copy.removeLayout(c);
                }
            }

            upsertForSegmentQuietly(copy);
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

    private NDataSegDetails upsertForSegment(NDataSegDetails details) {
        Preconditions.checkNotNull(details, "NDataSegDetails cannot be null.");

        getStore().checkAndPutResource(details.getResourcePath(), details, DATA_SEG_LAYOUT_INSTANCES_SERIALIZER);
        return details;
    }

    public NDataSegDetails updateDetails(NDataSegment seg, NDataSegDetailsUpdater updater) {
        NDataSegDetails details = copyForWrite(getForSegment(seg));
        updater.modify(details);
        upsertForSegment(details);
        return getForSegment(seg);
    }
    
    interface NDataSegDetailsUpdater {
        void modify(NDataSegDetails copyForWrite);
    }

    private void removeForSegmentQuietly(NDataflow df, String segId) {
        try {
            removeForSegment(segId);
        } catch (Exception e) {
            logger.error("Failed to remove NDataSegDetails for segment {}", df + "." + segId, e);
        }
    }

    /**
     * delete the segment from the restore.
     *
     * @param segId
     */
    void removeForSegment(String segId) {
        if (!getStore().exists(MetadataType.mergeKeyWithType(segId, LAYOUT))) {
            return;
        }

        getStore().deleteResource(MetadataType.mergeKeyWithType(segId, LAYOUT));
    }

    void removeDetails(NDataflow df) {
        val toBeRemoved = getStore().collectResourceRecursively(LAYOUT,
                RawResourceFilter.equalFilter("dataflowId", df.getUuid()));
        if (CollectionUtils.isNotEmpty(toBeRemoved)) {
            toBeRemoved.forEach(path -> getStore().deleteResource(path));
        }
    }
}
