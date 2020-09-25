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

package org.apache.kylin.stream.core.storage;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.stream.core.model.stats.LongLatencyInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class CheckPointStoreTest {
    private CheckPointStore cpStore;

    @Before
    public void setup() {
        String cubeName = "testCube";
        File checkPointFolder = new File(cubeName);
        if (!checkPointFolder.exists()) {
            checkPointFolder.mkdirs();
        } else {
            Assert.assertTrue(checkPointFolder.isDirectory());
        }
        cpStore = new CheckPointStore(cubeName, checkPointFolder);
    }

    @After
    public void cleanup() throws IOException {
        String cubeName = "testCube";
        File checkPointFolder = new File(cubeName);
        if (checkPointFolder.exists()) {
            FileUtils.deleteDirectory(checkPointFolder);
        }
    }

    @Test
    public void getLatestCheckPoint() throws IOException {
        CheckPoint cp1 = createCheckPoint1();
        CheckPoint cp2 = createCheckPoint2();
        cpStore.saveCheckPoint(cp1);
        cpStore.saveCheckPoint(cp2);

        CheckPoint cp3 = cpStore.getLatestCheckPoint();
        System.out.println(cp3);
    }

    @Test
    public void getLatestCheckPoint2() {
        CheckPoint cp3 = cpStore.getLatestCheckPoint();
        System.out.println(cp3);
    }

    @Test
    public void deleteOldCheckPointFiles() throws IOException {
        long DAY_TIMESTAMP_BASE = 24 * 3600 * 1000L;

        CheckPoint cp1 = createCheckPoint1();
        long startTime = cp1.getCheckPointTime();
        for (int i = 0; i < 10; i++) {
            cp1.setCheckPointTime(startTime + i * DAY_TIMESTAMP_BASE);
            cpStore.saveCheckPoint(cp1);
        }
        cpStore.deleteOldCPFiles();
        Assert.assertEquals(cpStore.getCheckPointFiles().length, CheckPointStore.CP_FILE_MAX_NUM);
    }

    public CheckPoint createCheckPoint1() throws IOException{
        CheckPoint cp = new CheckPoint();
        Map<Integer, Long> consumingStats = Maps.newHashMap();
        Map<Long, String> persistedIndexes = Maps.newHashMap();
        long currTime = System.currentTimeMillis();
        long currHour = (currTime / 3600000) * 3600000;
        consumingStats.put(1, 10000L);
        consumingStats.put(2, 20000L);
        consumingStats.put(3, 30000L);


        persistedIndexes.put(currHour, "1");
        persistedIndexes.put(currHour - 1, "2");
        persistedIndexes.put(currHour - 2, "3");

        cp.setCheckPointTime(System.currentTimeMillis());

        cp.setSourceConsumePosition(JsonUtil.writeValueAsString(consumingStats));
        cp.setPersistedIndexes(persistedIndexes);
        cp.setTotalCount(1000000);
        cp.setCheckPointCount(50000);
        cp.setLongLatencyInfo(new LongLatencyInfo());
        Map<Long, String> segmentSourceStartOffsets = Maps.newHashMap();
        Map<Integer, Long> sourceStartOffsets = Maps.newHashMap();
        sourceStartOffsets.put(1, 10000L);
        sourceStartOffsets.put(2, 20000L);
        sourceStartOffsets.put(3, 30000L);
        segmentSourceStartOffsets.put(currHour, JsonUtil.writeValueAsString(sourceStartOffsets));
        cp.setSegmentSourceStartPosition(segmentSourceStartOffsets);
        return cp;
    }

    public CheckPoint createCheckPoint2() throws IOException {
        CheckPoint cp = new CheckPoint();
        Map<Integer, Long> consumingStats = Maps.newHashMap();
        Map<Long, String> persistedIndexes = Maps.newHashMap();
        long currTime = System.currentTimeMillis() + 60000;
        long currHour = (currTime / 3600000) * 3600000;
        consumingStats.put(1, 30000L);
        consumingStats.put(2, 40000L);
        consumingStats.put(3, 50000L);

        persistedIndexes.put(currHour, "1");
        persistedIndexes.put(currHour - 1, "2");
        persistedIndexes.put(currHour - 2, "3");

        cp.setCheckPointTime(System.currentTimeMillis());

        cp.setSourceConsumePosition(JsonUtil.writeValueAsString(consumingStats));
        cp.setPersistedIndexes(persistedIndexes);
        cp.setTotalCount(1000000);
        cp.setCheckPointCount(50000);
        cp.setLongLatencyInfo(new LongLatencyInfo());
        Map<Long, String> segmentSourceStartOffsets = Maps.newHashMap();
        Map<Integer, Long> sourceStartOffsets = Maps.newHashMap();
        sourceStartOffsets.put(1, 10000L);
        sourceStartOffsets.put(2, 20000L);
        sourceStartOffsets.put(3, 30000L);
        segmentSourceStartOffsets.put(currHour, JsonUtil.writeValueAsString(sourceStartOffsets));
        cp.setSegmentSourceStartPosition(segmentSourceStartOffsets);
        return cp;
    }

}
