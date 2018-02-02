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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.cachesync.Broadcaster.BroadcastEvent;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.Broadcaster.Listener;
import org.apache.kylin.metadata.cachesync.Broadcaster.SyncErrorHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BroadcasterTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        Broadcaster broadcaster = Broadcaster.getInstance(getTestConfig());
        final AtomicInteger i = new AtomicInteger(0);

        broadcaster.registerStaticListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                Assert.assertEquals(2, i.incrementAndGet());
            }
        }, "test");

        broadcaster.registerListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                Assert.assertEquals(1, i.incrementAndGet());
            }
        }, "test");

        broadcaster.notifyListener("test", Event.UPDATE, "");

        broadcaster.stopAnnounce();
        Broadcaster.staticListenerMap.clear();
    }

    @Test
    public void testNotifyNonStatic() throws IOException {
        Broadcaster broadcaster = Broadcaster.getInstance(getTestConfig());
        final AtomicInteger i = new AtomicInteger(0);

        broadcaster.registerStaticListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                throw new IllegalStateException("Should not notify static listener.");
            }
        }, "test");

        broadcaster.registerListener(new Listener() {
            @Override
            public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                    throws IOException {
                Assert.assertEquals(1, i.incrementAndGet());
            }
        }, "test");

        broadcaster.notifyNonStaticListener("test", Event.UPDATE, "");

        broadcaster.stopAnnounce();
        Broadcaster.staticListenerMap.clear();
    }

    @Test
    public void testAnnounceErrorHandler() throws IOException, InterruptedException {
        System.setProperty("kylin.server.cluster-servers", "localhost:717");
        System.setProperty("kylin.metadata.sync-error-handler", MockupErrHandler.class.getName());
        try {
            Broadcaster broadcaster = Broadcaster.getInstance(getTestConfig());

            broadcaster.announce("all", "update", "all");
            
            for (int i = 0; i < 30 && MockupErrHandler.atom.get() == 0; i++) {
                Thread.sleep(1000);
            }

            broadcaster.stopAnnounce();
            Broadcaster.staticListenerMap.clear();
        } finally {
            System.clearProperty("kylin.server.cluster-servers");
            System.clearProperty("kylin.metadata.sync-error-handler");
        }
        
        Assert.assertTrue(MockupErrHandler.atom.get() > 0);
    }
    
    public static class MockupErrHandler implements SyncErrorHandler {
        static AtomicInteger atom = new AtomicInteger();
        
        @Override
        public void init(Broadcaster broadcaster) {
        }

        @Override
        public void handleAnnounceError(String targetNode, RestClient restClient, BroadcastEvent event) {
            Assert.assertEquals("localhost:717", targetNode);
            atom.incrementAndGet();
        }
        
    }
}
