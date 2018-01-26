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

package org.apache.kylin.rest.service;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.query.security.QueryACLTestUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageFactory;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KyilnQueryTimeoutTest extends LocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.createTestMetadata();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.storage.provider.2", MockQueryTimeoutStorage.class.getName());
        config.setProperty("kylin.storage.default", "2");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        StorageFactory.clearCache();
    }

    @Test
    public void testQueryTimeout() throws SQLException {
        thrown.expectCause(CoreMatchers.isA(KylinTimeoutException.class));
        thrown.expectMessage(CoreMatchers.containsString("Kylin query timeout"));
        StorageFactory.clearCache();
        BadQueryDetector detector = new BadQueryDetector(100, BadQueryDetector.getSystemAvailMB() * 2, 100, 1);
        detector.start();
        SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql("select count(*) from STREAMING_TABLE");
        detector.queryStart(Thread.currentThread(), request, "ADMIN");
        try {
            QueryACLTestUtil.mockQuery("default", "select * from STREAMING_TABLE");
        } finally{
            detector.queryEnd(Thread.currentThread(), "timeout");
            detector.interrupt();
        }
    }

    public static class MockQueryTimeoutStorage implements IStorage {

        @Override
        public IStorageQuery createQuery(IRealization realization) {
            return new MockQueryTimeoutQuery();
        }

        @Override
        public <I> I adaptToBuildEngine(Class<I> engineInterface) {
            return null;
        }
    }

    private static class MockQueryTimeoutQuery implements IStorageQuery {
        @Override
        public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                throw new KylinTimeoutException("Kylin query timeout");
            }
            return null;
        }
    }
}