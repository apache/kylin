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

package org.apache.kylin.metadata.draft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DraftManager {

    private static final Logger logger = LoggerFactory.getLogger(DraftManager.class);

    public static final Serializer<Draft> DRAFT_SERIALIZER = new JsonSerializer<Draft>(Draft.class);

    public static DraftManager getInstance(KylinConfig config) {
        return config.getManager(DraftManager.class);
    }

    // called by reflection
    static DraftManager newInstance(KylinConfig config) throws IOException {
        return new DraftManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    private DraftManager(KylinConfig config) throws IOException {
        this.config = config;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public List<Draft> list(String project) throws IOException {
        List<Draft> result = new ArrayList<>();
        ResourceStore store = getStore();
        NavigableSet<String> listPath = store.listResources(ResourceStore.DRAFT_RESOURCE_ROOT);
        if (listPath == null)
            return result;
        
        for (String path : listPath) {
            Draft draft = store.getResource(path, DRAFT_SERIALIZER);
            
            if (draft == null)
                continue;
            
            if (project == null || project.equals(draft.getProject()))
                result.add(draft);
        }
        return result;
    }

    public void save(String project, String uuid, RootPersistentEntity... entities) throws IOException {
        Draft draft = new Draft();
        draft.setProject(project);
        draft.setUuid(uuid);
        draft.setEntities(entities);
        save(draft);
    }

    public void save(Draft draft) throws IOException {
        if (draft.getUuid() == null) {
            throw new IllegalArgumentException();
        }

        Draft youngerSelf = load(draft.getUuid());
        if (youngerSelf != null) {
            draft.setLastModified(youngerSelf.getLastModified());
        }

        ResourceStore store = getStore();
        store.checkAndPutResource(draft.getResourcePath(), draft, DRAFT_SERIALIZER);
        
        logger.trace("Saved " + draft);
    }

    public Draft load(String uuid) throws IOException {
        ResourceStore store = getStore();
        Draft draft = store.getResource(Draft.concatResourcePath(uuid), DRAFT_SERIALIZER);
        return draft;
    }

    public void delete(String uuid) throws IOException {
        ResourceStore store = getStore();
        store.deleteResource(Draft.concatResourcePath(uuid));
    }
}
