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

package org.apache.kylin.dict;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ITGlobalDictionaryBuilderTest extends HBaseMetadataTestCase {
    private DictionaryInfo dictionaryInfo;

    @Before
    public void beforeTest() throws Exception {
        staticCreateTestMetadata();
        dictionaryInfo = new DictionaryInfo("testTable", "testColumn", 0, "String", null);
    }

    @After
    public void afterTest() {
        cleanup();
        staticCleanupTestMetadata();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private void cleanup() {
        String BASE_DIR = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/resources/GlobalDict"
                + dictionaryInfo.getResourceDir() + "/";
        Path basePath = new Path(BASE_DIR);
        try {
            HadoopUtil.getFileSystem(basePath).delete(basePath, true);
        } catch (IOException e) {
        }
    }

    @Test
    public void testGlobalDictLock() throws IOException, InterruptedException {
        final CountDownLatch startLatch = new CountDownLatch(3);
        final CountDownLatch finishLatch = new CountDownLatch(3);

        Thread t1 = new SharedBuilderThread(startLatch, finishLatch, "t1_", 10000);
        Thread t2 = new SharedBuilderThread(startLatch, finishLatch, "t2_", 10);
        Thread t3 = new SharedBuilderThread(startLatch, finishLatch, "t3_", 100000);
        t1.start();
        t2.start();
        t3.start();
        startLatch.await();
        finishLatch.await();

        GlobalDictionaryBuilder builder = new GlobalDictionaryBuilder();
        builder.init(dictionaryInfo, 0, KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
        builder.addValue("success");
        Dictionary<String> dict = builder.build();

        for (int i = 0; i < 10000; i++) {
            Assert.assertNotEquals(-1, dict.getIdFromValue("t1_" + i));
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertNotEquals(-1, dict.getIdFromValue("t2_" + i));
        }
        for (int i = 0; i < 100000; i++) {
            Assert.assertNotEquals(-1, dict.getIdFromValue("t3_" + i));
        }

        Assert.assertEquals(110011, dict.getIdFromValue("success"));
    }

    @Test
    public void testBuildGlobalDictFailed() throws IOException {
        thrown.expect(IOException.class);
        thrown.expectMessage("read failed.");

        GlobalDictionaryBuilder builder = new GlobalDictionaryBuilder();
        try {
            DictionaryGenerator.buildDictionary(builder, dictionaryInfo, new ErrorDictionaryValueEnumerator());
        } catch (Throwable e) {
            DistributedLock lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
            String lockPath = "/dict/" + dictionaryInfo.getSourceTable() + "_" + dictionaryInfo.getSourceColumn()
                    + "/lock";
            Assert.assertFalse(lock.isLocked(lockPath));
            throw e;
        }
    }

    private class SharedBuilderThread extends Thread {
        CountDownLatch startLatch;
        CountDownLatch finishLatch;
        String prefix;
        int count;

        SharedBuilderThread(CountDownLatch startLatch, CountDownLatch finishLatch, String prefix, int count) {
            this.startLatch = startLatch;
            this.finishLatch = finishLatch;
            this.prefix = prefix;
            this.count = count;
        }

        @Override
        public void run() {
            try {
                GlobalDictionaryBuilder builder = new GlobalDictionaryBuilder();
                startLatch.countDown();

                builder.init(dictionaryInfo, 0, KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
                for (int i = 0; i < count; i++) {
                    builder.addValue(prefix + i);
                }
                builder.build();
                finishLatch.countDown();
            } catch (IOException e) {
            }
        }
    }

    private class ErrorDictionaryValueEnumerator implements IDictionaryValueEnumerator {
        private int idx = 0;

        @Override
        public String current() throws IOException {
            return null;
        }

        @Override
        public boolean moveNext() throws IOException {
            idx++;
            if (idx == 1)
                throw new IOException("read failed.");
            return true;
        }

        @Override
        public void close() throws IOException {

        }
    }
}