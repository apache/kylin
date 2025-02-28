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

package org.apache.kylin.metadata;

import java.util.Collections;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.ComputedColumnManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.query.QueryRecord;
import org.apache.kylin.rest.model.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetadataInfo(onlyProps = true)
class ManagerTest {

    @Test
    void testDynamicManager() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Manager<QueryRecord> manager = Manager.getInstance(config, "default", QueryRecord.class);
        Manager<QueryRecord> manager2 = Manager.getInstance(config, "default", QueryRecord.class);
        Manager<QueryRecord> manager3 = Manager.getInstance(config, "default2", QueryRecord.class);

        Assertions.assertEquals(manager, manager2);
        Assertions.assertNotEquals(manager, manager3);
    }

    @Test
    void testHappyPath() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String project = "default";
        String resourceName = "default.admin";
        Manager<QueryRecord> manager = Manager.getInstance(config, project, QueryRecord.class);

        manager.upsert(resourceName, copyForWrite -> {
            copyForWrite.setQueries(Collections.singletonList(new Query()));
        }, () -> new QueryRecord(project, "admin"));
        QueryRecord queryRecord = manager.get(resourceName).orElse(null);
        Assertions.assertNotNull(queryRecord);
        Assertions.assertEquals(1, queryRecord.getQueries().size());

        manager.upsert(resourceName, copyForWrite -> {
            copyForWrite.setQueries(Collections.emptyList());
        }, () -> new QueryRecord(project, "admin"));
        queryRecord = manager.get(resourceName).orElse(null);
        Assertions.assertNotNull(queryRecord);
        Assertions.assertTrue(queryRecord.getQueries().isEmpty());

        manager.update(resourceName, copyForWrite -> {
            copyForWrite.setQueries(Collections.singletonList(new Query()));
        });
        queryRecord = manager.get(resourceName).orElse(null);
        Assertions.assertNotNull(queryRecord);
        Assertions.assertEquals(1, queryRecord.getQueries().size());

        queryRecord = manager.get("abc").orElse(null);
        Assertions.assertNull(queryRecord);

        queryRecord = manager.get(null).orElse(null);
        Assertions.assertNull(queryRecord);
        
        Assertions.assertFalse(manager.listAll().isEmpty());
        Assertions.assertFalse(manager.listByFilter(new RawResourceFilter()).isEmpty());

        manager.delete(manager.get(resourceName).orElse(null));
        Assertions.assertTrue(manager.listAll().isEmpty());
    }

    @Test
    void testIllegalOperations() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String project = "default";
        Manager<QueryRecord> manager = Manager.getInstance(config, project, QueryRecord.class);
        QueryRecord queryRecord = new QueryRecord(project, "admin");
        QueryRecord record2 = new QueryRecord(project, "admin2");
        QueryRecord recordWithoutUuid = new QueryRecord(project, "admin");
        recordWithoutUuid.setUuid(null);
        QueryRecord recordCached = new QueryRecord(project, "admin");
        recordCached.setCachedAndShared(true);


        // test unsupported manager class
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Manager.getInstance(config, null, NDataModel.class));
        // test create entity without uuid
        Assertions.assertThrows(IllegalArgumentException.class, () -> manager.createAS(recordWithoutUuid));
        // test create an existing entity
        manager.createAS(queryRecord);
        Assertions.assertThrows(IllegalArgumentException.class, () -> manager.createAS(queryRecord));
        

        CustomManager customManager = config.getManager(project, CustomManager.class);
        // test update to a new entity without uuid
        Assertions.assertThrows(IllegalArgumentException.class, () -> customManager.update(recordWithoutUuid));
        // test update to a new cached entity
        Assertions.assertThrows(IllegalStateException.class, () -> customManager.update(recordCached));
        // test update a not existing entity
        Assertions.assertThrows(IllegalArgumentException.class, () -> customManager.update(record2));
    }
    
    public static class CustomManager extends Manager<QueryRecord> {

        private static final Logger logger = LoggerFactory.getLogger(ComputedColumnManager.class);

        protected CustomManager(KylinConfig cfg, String project) {
            super(cfg, project, MetadataType.QUERY_RECORD);
        }

        static CustomManager newInstance(KylinConfig config, String project) {
            return new CustomManager(config, project);
        }

        @Override
        public Logger logger() {
            return logger;
        }

        @Override
        public String name() {
            return "CustomManager";
        }

        @Override
        public Class<QueryRecord> entityType() {
            return QueryRecord.class;
        }

        QueryRecord update(QueryRecord entity) {
            return internalUpdate(entity);
        }
    }
}
