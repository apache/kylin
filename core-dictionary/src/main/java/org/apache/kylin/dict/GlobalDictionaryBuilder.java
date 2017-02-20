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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.global.AppendTrieDictionaryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;

/**
 * GlobalDictinary based on whole cube, to ensure one value has same dict id in different segments.
 * GlobalDictinary mainly used for count distinct measure to support rollup among segments.
 * Created by sunyerui on 16/5/24.
 */
public class GlobalDictionaryBuilder implements IDictionaryBuilder {
    private AppendTrieDictionaryBuilder builder;
    private int baseId;

    private DistributedLock lock;
    private String sourceColumn;
    private final String lockData = getServerName() + "_" + Thread.currentThread().getName();
    private int counter;

    private static Logger logger = LoggerFactory.getLogger(GlobalDictionaryBuilder.class);

    @Override
    public void init(DictionaryInfo dictInfo, int baseId) throws IOException {
        if (dictInfo == null) {
            throw new IllegalArgumentException("GlobalDictinaryBuilder must used with an existing DictionaryInfo");
        }

        sourceColumn = dictInfo.getSourceTable() + "_" + dictInfo.getSourceColumn();
        lock(sourceColumn);

        int maxEntriesPerSlice = KylinConfig.getInstanceFromEnv().getAppendDictEntrySize();
        this.builder = new AppendTrieDictionaryBuilder(dictInfo.getResourceDir(), maxEntriesPerSlice);
        this.baseId = baseId;
    }

    @Override
    public boolean addValue(String value) {
        if (++counter % 1_000_000 == 0) {
            if (lock.lockPath(getLockPath(sourceColumn), lockData)) {
                logger.info("processed {} values for {}", counter, sourceColumn);
            } else {
                throw new RuntimeException("Failed to create global dictionary on " + sourceColumn + " This client doesn't keep the lock");
            }
        }

        if (value == null) {
            return false;
        }

        try {
            builder.addValue(value);
        } catch (Throwable e) {
            lock.unlockPath(getLockPath(sourceColumn), lockData);
            throw new RuntimeException(String.format("Failed to create global dictionary on %s ", sourceColumn), e);
        }

        return true;
    }

    @Override
    public Dictionary<String> build() throws IOException {
        try {
            if (lock.lockPath(getLockPath(sourceColumn), lockData)) {
                return builder.build(baseId);
            }
        } finally {
            lock.unlockPath(getLockPath(sourceColumn), lockData);
        }
        return new AppendTrieDictionary<>();
    }

    private void lock(final String sourceColumn) throws IOException {
        lock = KylinConfig.getInstanceFromEnv().getDistributedLock();

        if (!lock.lockPath(getLockPath(sourceColumn), lockData)) {
            logger.info("{} will wait the lock for {} ", lockData, sourceColumn);

            final BlockingQueue<String> bq = new ArrayBlockingQueue<String>(1);

            Closeable watch = lock.watchPath(getWatchPath(sourceColumn), MoreExecutors.sameThreadExecutor(), new DistributedLock.Watcher() {
                @Override
                public void process(String path, String data) {
                    if (!data.equalsIgnoreCase(lockData) && lock.lockPath(getLockPath(sourceColumn), lockData)) {
                        try {
                            bq.put("getLock");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });

            long start = System.currentTimeMillis();

            try {
                bq.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                watch.close();
            }

            logger.info("{} has waited the lock {} ms for {} ", lockData, (System.currentTimeMillis() - start), sourceColumn);
        }
    }

    private static final String GLOBAL_DICT_LOCK_PATH = "/kylin/dict/lock";

    private String getLockPath(String pathName) {
        return GLOBAL_DICT_LOCK_PATH + "/" + KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix() + "/" + pathName + "/lock";
    }

    private String getWatchPath(String pathName) {
        return GLOBAL_DICT_LOCK_PATH + "/" + KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix() + "/" + pathName;
    }

    private static String getServerName() {
        String serverName = null;
        try {
            serverName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("fail to get the serverName");
        }
        return serverName;
    }
}
