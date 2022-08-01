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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.SneakyThrows;
import lombok.val;

@MetadataInfo(onlyProps = true)
public class MessageSynchronizationTest {

    private final Charset charset = Charset.defaultCharset();

    @Test
    public void replayTest() {
        val synchronize = MessageSynchronization.getInstance(getTestConfig());
        val events = createEvents();
        synchronize.replayInTransaction(new UnitMessages(events));
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val raw = resourceStore.getResource("/default/abc.json");
        Assert.assertEquals(1, raw.getMvcc());
        val empty = resourceStore.getResource("/default/abc3.json");
        Assert.assertNull(empty);
    }

    @OverwriteProp(key = "kylin.server.mode", value = "query")
    @Test
    public void testKE19979() throws InterruptedException {
        AtomicInteger mvcc = new AtomicInteger(0);
        val initEvent = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc.json", ByteSource.wrap("version1".getBytes(charset)), 0L, mvcc.get()));
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
                    val updateEvent = new ResourceCreateOrUpdateEvent(new RawResource("/default/abc.json",
                            ByteSource.wrap("version2".getBytes(charset)), 0L, mvcc.incrementAndGet()));
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
                    if (null == resourceStore.getResource("/default/abc.json")) {
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
        Assert.assertEquals(0, nullCount.get());
    }

    private List<Event> createEvents() {
        val event1 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc.json", ByteSource.wrap("version1".getBytes(charset)), 0L, 0));
        val event2 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc2.json", ByteSource.wrap("abc2".getBytes(charset)), 0L, 0));
        val event3 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc.json", ByteSource.wrap("version2".getBytes(charset)), 0L, 1));
        val event4 = new ResourceCreateOrUpdateEvent(
                new RawResource("/default/abc3.json", ByteSource.wrap("42".getBytes(charset)), 0L, 0));
        val event5 = new ResourceDeleteEvent("/default/abc3.json");
        return Lists.newArrayList(event1, event2, event3, event4, event5).stream().peek(e -> e.setKey("default"))
                .collect(Collectors.toList());
    }

}
