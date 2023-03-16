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
package org.apache.kylin.metadata.cachesync;

import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.val;

@Builder
@AllArgsConstructor
public class CacheReloadChecker<T extends RootPersistentEntity> {

    private ResourceStore store;

    private CachedCrudAssist<T> crud;

    boolean needReload(String resourceName) {
        val entity = crud.getCache().getIfPresent(resourceName);
        if (entity == null) {
            return true;
        } else {
            return checkDependencies(entity);
        }
    }

    private boolean checkDependencies(RootPersistentEntity entity) {
        val raw = store.getResource(entity.getResourcePath());
        if (raw == null) {
            // if still missing, no need to reload
            return !(entity instanceof MissingRootPersistentEntity);
        }
        if (raw.getMvcc() != entity.getMvcc()) {
            return true;
        }

        Preconditions.checkState(!(entity instanceof MissingRootPersistentEntity));

        val entities = entity.getDependencies();
        if (entities == null) {
            return false;
        }
        for (val depEntity : entities) {
            val depNeedReload = checkDependencies(depEntity);
            if (depNeedReload) {
                return true;
            }
        }
        return false;
    }

}
