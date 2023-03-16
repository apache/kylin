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
package org.apache.kylin.metadata.upgrade;

import static org.apache.kylin.common.persistence.ResourceStore.UPGRADE;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

public class GlobalAclVersionManager {
    private static final Logger logger = LoggerFactory.getLogger(GlobalAclVersionManager.class);

    public static GlobalAclVersionManager getInstance(KylinConfig config) {
        return config.getManager(GlobalAclVersionManager.class);
    }

    // called by reflection
    static GlobalAclVersionManager newInstance(KylinConfig config) {
        return new GlobalAclVersionManager(config);
    }

    private KylinConfig config;
    private CachedCrudAssist<GlobalAclVersion> crud;

    public GlobalAclVersionManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing AclVersionManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.crud = new CachedCrudAssist<GlobalAclVersion>(getStore(), UPGRADE, GlobalAclVersion.class) {
            @Override
            protected GlobalAclVersion initEntityAfterReload(GlobalAclVersion globalAclVersion, String resourceName) {
                return globalAclVersion;
            }
        };
        this.crud.reloadAll();
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public boolean exists() {
        return crud.exists(GlobalAclVersion.VERSION_KEY_NAME);
    }

    public void save(GlobalAclVersion globalAclVersion) {
        Preconditions.checkNotNull(globalAclVersion.getAclVersion());
        if (!exists()) {
            crud.save(globalAclVersion);
        }
    }

    public void delete() {
        if (exists()) {
            crud.delete(GlobalAclVersion.VERSION_KEY_NAME);
        }
    }
}
