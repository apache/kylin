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
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.global.AppendTrieDictionaryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private int counter;

    private static Logger logger = LoggerFactory.getLogger(GlobalDictionaryBuilder.class);

    @Override
    public void init(DictionaryInfo dictInfo, int baseId, String hdfsDir) throws IOException {
        sourceColumn = dictInfo.getSourceTable() + "_" + dictInfo.getSourceColumn();
        int maxEntriesPerSlice = KylinConfig.getInstanceFromEnv().getAppendDictEntrySize();
        if (hdfsDir == null) {
            //build in Kylin job server
            hdfsDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        }
        String baseDir = hdfsDir + "resources/GlobalDict" + dictInfo.getResourceDir() + "/";

        lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
        String lockPath = getLockPath(sourceColumn);
        try {
            lock.lock(lockPath, Long.MAX_VALUE);
            this.builder = new AppendTrieDictionaryBuilder(baseDir, maxEntriesPerSlice, true);
        } catch (Throwable e) {
            throw new RuntimeException(
                    String.format(Locale.ROOT, "Failed to create global dictionary on %s ", sourceColumn), e);
        } finally {
            if (lock.isLockedByMe(lockPath)) {
                lock.unlock(lockPath);
            }
        }
        this.baseId = baseId;
    }

    @Override
    public boolean addValue(String value) {
        if (++counter % 1_000_000 == 0) {
            if (lock.lock(getLockPath(sourceColumn))) {
                logger.info("processed {} values for {}", counter, sourceColumn);
            } else {
                throw new RuntimeException(
                        "Failed to create global dictionary on " + sourceColumn + " This client doesn't keep the lock");
            }
        }

        if (value == null) {
            return false;
        }

        try {
            builder.addValue(value);
        } catch (Throwable e) {
            lock.unlock(getLockPath(sourceColumn));
            throw new RuntimeException(
                    String.format(Locale.ROOT, "Failed to create global dictionary on %s ", sourceColumn), e);
        }

        return true;
    }

    @Override
    public Dictionary<String> build() throws IOException {
        try {
            if (lock.lock(getLockPath(sourceColumn))) {
                return builder.build(baseId);
            }
        } finally {
            lock.unlock(getLockPath(sourceColumn));
        }
        return new AppendTrieDictionary<>();
    }

    @Override
    public void clear() {
        if (lock.isLocked(getLockPath(sourceColumn))) {
            lock.unlock(getLockPath(sourceColumn));
        }
    }

    private String getLockPath(String pathName) {
        return "/dict/" + pathName + "/lock";
    }
}
