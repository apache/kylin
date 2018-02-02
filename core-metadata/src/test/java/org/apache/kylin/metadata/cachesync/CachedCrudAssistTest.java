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

import java.io.IOException;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CachedCrudAssistTest extends LocalFileMetadataTestCase {
    @Before
    public void setup() throws Exception {
        staticCreateTestMetadata();
    }

    @Test
    public void testSave() throws IOException {
        CachedCrudAssist<TestEntity> crudAssist = new CachedCrudAssist<TestEntity>(getStore() //
                , "/test" //
                , TestEntity.class //
                , new CaseInsensitiveStringCache<TestEntity>(getTestConfig(), "test")) { //
            @Override
            protected TestEntity initEntityAfterReload(TestEntity entity, String resourceName) {
                return entity;
            }
        };

        TestEntity entity = new TestEntity();
        Assert.assertNull(entity.getUuid());
        crudAssist.save(entity);
        Assert.assertNotNull(entity.getUuid());
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }
}

class TestEntity extends RootPersistentEntity {
    public TestEntity() {
    }
}