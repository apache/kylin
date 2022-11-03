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
package org.apache.kylin.tool.routine;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.kylin.tool.MaintainModeTool;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
public class MaintainModeToolTest {

    @Test
    public void testForceToExit() {
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-off", "--force" });
    }

    @Test
    public void testEnterMaintainMode() {
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-on", "-reason", "test" });
    }

    @Test
    public void testEnterMaintainModeEpochCheck() {
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, true);

        val globalEpoch = epochManager.getGlobalEpoch();
        int id = 1234;
        globalEpoch.setEpochId(id);

        ReflectionTestUtils.invokeMethod(epochManager, "insertOrUpdateEpoch", globalEpoch);

        Assert.assertEquals(epochManager.getGlobalEpoch().getEpochId(), id);
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-on", "-reason", "test" });

        Assert.assertEquals(epochManager.getGlobalEpoch().getEpochId(), id + 1);

        maintainModeTool.execute(new String[] { "-off", "--force" });
        Assert.assertEquals(epochManager.getGlobalEpoch().getEpochId(), id + 2);
    }

    @Test
    public void testCleanProject() {
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-on", "-reason", "test", "-p", "notExistP1" });

        Assert.assertEquals(getEpochStore().list().size(), 2);
    }

    EpochStore getEpochStore() {
        try {
            return EpochStore.getEpochStore(getTestConfig());
        } catch (Exception e) {
            throw new RuntimeException("cannot init epoch store!");
        }
    }
}
