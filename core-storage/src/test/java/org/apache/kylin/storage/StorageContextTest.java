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

package org.apache.kylin.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.kylin.gridtable.StorageLimitLevel;
import org.junit.Test;

/**
 * Unit tests for class {@link StorageContext}.
 *
 * @see StorageContext
 */
public class StorageContextTest {


    @Test
    public void testSetLimit() {
        StorageContext storageContext = new StorageContext();
        storageContext.setLimit(10);

        assertEquals(StorageLimitLevel.NO_LIMIT, storageContext.getStorageLimitLevel());
        assertFalse(storageContext.isLimitPushDownEnabled());
        assertEquals(Integer.MAX_VALUE, storageContext.getFinalPushDownLimit());

        storageContext.setLimit(0);

        assertEquals(StorageLimitLevel.NO_LIMIT, storageContext.getStorageLimitLevel());
        assertFalse(storageContext.isLimitPushDownEnabled());
        assertEquals(Integer.MAX_VALUE, storageContext.getFinalPushDownLimit());
    }


    @Test
    public void testApplyLimitPushDownUsingStorageLimitLevelNO_LIMIT() {
        StorageContext storageContext = new StorageContext();
        storageContext.setLimit(10);
        storageContext.applyLimitPushDown(null, StorageLimitLevel.NO_LIMIT);

        assertEquals(StorageLimitLevel.NO_LIMIT, storageContext.getStorageLimitLevel());
        assertFalse(storageContext.isLimitPushDownEnabled());
        assertEquals(Integer.MAX_VALUE, storageContext.getFinalPushDownLimit());
    }


}
