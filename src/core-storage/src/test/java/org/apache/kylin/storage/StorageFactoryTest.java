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

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.realization.IRealization;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageFactoryTest {
    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty("kylin.storage.provider.0", MockupStorageEngine.class.getName());

        KylinConfig.setKylinConfigInEnvIfMissing(props);
    }

    @Test
    public void testSingleThread() {
        IStorage s1 = StorageFactory.storage(new MockupStorageAware());
        IStorage s2 = StorageFactory.storage(new MockupStorageAware());

        Assert.assertSame(s1, s2);
    }

    @Test
    public void testMultipleThread() throws InterruptedException {
        final IStorage[] s = new IStorage[2];

        // thread 1
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                s[0] = StorageFactory.storage(new MockupStorageAware());
            }
        });
        t.start();
        t.join();

        // thread 2
        t = new Thread(new Runnable() {
            @Override
            public void run() {
                s[1] = StorageFactory.storage(new MockupStorageAware());
            }
        });
        t.start();
        t.join();

        Assert.assertNotSame(s[0], s[1]);
    }

    class MockupStorageAware implements IStorageAware {
        @Override
        public int getStorageType() {
            return 0;
        }
    }

    public static class MockupStorageEngine implements IStorage {

        @Override
        public IStorageQuery createQuery(IRealization realization) {
            return null;
        }

        @Override
        public <I> I adaptToBuildEngine(Class<I> engineInterface) {
            return null;
        }
    }
}
