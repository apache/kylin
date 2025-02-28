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

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import lombok.extern.slf4j.Slf4j;

import static org.apache.kylin.metadata.cube.model.NDataLayoutDetails.SEPARATOR;

@Slf4j
public class NDataLayoutDetailsManager {

    public static NDataLayoutDetailsManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataLayoutDetailsManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataLayoutDetailsManager newInstance(KylinConfig config, String project) {
        return new NDataLayoutDetailsManager(config, project);
    }

    // ============================================================================

    private KylinConfig kylinConfig;
    private final String project;
    private CachedCrudAssist<NDataLayoutDetails> crud;

    private NDataLayoutDetailsManager(KylinConfig config, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            log.info("Initializing NDataLayoutDetailsManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(config), project);
        this.kylinConfig = config;
        this.project = project;
        this.crud = new CachedCrudAssist<NDataLayoutDetails>(getStore(), MetadataType.LAYOUT_DETAILS, project,
                NDataLayoutDetails.class) {
            @Override
            protected NDataLayoutDetails initEntityAfterReload(NDataLayoutDetails entity, String resourceName) {
                return entity;
            }
        };
        this.crud.setCheckCopyOnWrite(true);
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    public KylinConfig getConfig() {
        return kylinConfig;
    }

    public List<NDataLayoutDetails> listNDataLayoutDetailsByModel(String modelId) {
        return crud.listByFilter(RawResourceFilter.equalFilter("dataflowId", modelId));
    }

    public NDataLayoutDetails getNDataLayoutDetails(String modelId, long layoutId) {
        return crud.get(modelId + SEPARATOR + layoutId);
    }

    public void save(NDataLayoutDetails fragment) {
        crud.save(fragment);
    }

    public interface NDataLayoutDetailsUpdater {
        void modify(NDataLayoutDetails copyForWrite);
    }

    public void updateLayoutDetails(String modelId, long layoutId, NDataLayoutDetailsUpdater updater) {
        NDataLayoutDetails details = new NDataLayoutDetails();
        details.setProject(project);
        details.setModelId(modelId);
        details.setLayoutId(layoutId);
        if (crud.contains(details.resourceName())) {
            details = crud.get(details.resourceName());
        }
        details = copyForWrite(details);
        updater.modify(details);
        crud.save(details);
    }

    public void removeFragmentBySegment(NDataflow df, NDataSegment segment) {
        String modelId = df.getId();
        List<LayoutEntity> layoutId = df.getIndexPlan().getAllLayouts();
        for (LayoutEntity layoutEntity : layoutId) {
            updateLayoutDetails(modelId, layoutEntity.getId(), (copyForWrite) -> {
                copyForWrite.getFragmentRangeSet().remove(segment.getRange());
            });
        }
    }

    public void removeDetails(String modelId, Set<Long> layoutIds) {
        for (long layoutId : layoutIds) {
            crud.delete(modelId + SEPARATOR + layoutId);
        }
    }

    public NDataLayoutDetails copyForWrite(NDataLayoutDetails details) {
        Preconditions.checkNotNull(details);
        return crud.copyForWrite(details);
    }
}
