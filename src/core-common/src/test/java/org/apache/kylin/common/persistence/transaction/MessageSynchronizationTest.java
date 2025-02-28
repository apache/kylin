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
package org.apache.kylin.common.persistence.transaction;

import static org.apache.kylin.common.persistence.metadata.JdbcAuditLogStoreTool.createEvents;
import static org.apache.kylin.common.persistence.metadata.JdbcAuditLogStoreTool.createProjectAuditLog;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import lombok.val;

@MetadataInfo(onlyProps = true)
public class MessageSynchronizationTest {

    @Test
    public void replayTest() {
        val synchronize = MessageSynchronization.getInstance(getTestConfig());
        val events = createEvents();
        synchronize.replayInTransaction(new UnitMessages(events));
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val raw = resourceStore.getResource("PROJECT/abc");
        Assertions.assertEquals(1, raw.getMvcc());
        val empty = resourceStore.getResource("PROJECT/abc3");
        Assertions.assertNull(empty);
    }

    @OverwriteProp(key = "kylin.server.mode", value = "query")
    @Test
    public void testKE19979() throws InterruptedException {
        AtomicInteger mvcc = new AtomicInteger(0);
        val initEvent = (ResourceCreateOrUpdateEvent) Event.fromLog(createProjectAuditLog("abc", 0));
        val synchronize = MessageSynchronization.getInstance(getTestConfig());
        synchronize.replayInTransaction(new UnitMessages(Lists.newArrayList(initEvent)));
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val loopTime = 1000;
        val starter = new CountDownLatch(1);
        val latch1 = new CountDownLatch(loopTime);
        val latch2 = new CountDownLatch(loopTime);
        AtomicInteger nullCount = new AtomicInteger(0);
        Thread t1 = new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                starter.await();
                while (latch1.getCount() > 0) {
                    val updateEvent = (ResourceCreateOrUpdateEvent) Event
                            .fromLog(createProjectAuditLog("abc", mvcc.incrementAndGet()));
                    synchronize.replayInTransaction(new UnitMessages(Lists.newArrayList(updateEvent)));
                    latch1.countDown();
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                starter.await();
                while (latch2.getCount() > 0) {
                    if (null == resourceStore.getResource("PROJECT/abc")) {
                        nullCount.incrementAndGet();
                    }
                    latch2.countDown();
                }
            }
        });
        t1.start();
        t2.start();
        starter.countDown();
        latch1.await();
        latch2.await();
        Assertions.assertEquals(0, nullCount.get());
    }

}
