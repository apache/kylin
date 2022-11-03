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

package org.apache.kylin.tool;

import static org.apache.kylin.common.KylinExternalConfigLoader.KYLIN_CONF_PROPERTIES_FILE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*",
        "javax.crypto.*" })
@PrepareForTest({ DigestUtils.class })
public class KylinConfigChecksumCLITest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testErrorArgs() {
        int ret = KylinConfigChecksumCLI.execute(new String[] {});
        assertEquals(-1, ret);
    }

    @Test
    public void testCorrectChecksum4PropOverrideNotExists() throws Exception {
        File kylinConfDir = KylinConfig.getKylinConfDir();
        File propFile = new File(kylinConfDir, KYLIN_CONF_PROPERTIES_FILE);
        File propOverrideFile = new File(kylinConfDir, KYLIN_CONF_PROPERTIES_FILE + ".override");
        String propContent = FileUtils.readFileToString(propFile, StandardCharsets.UTF_8);
        if (propOverrideFile.exists()) {
            propContent += FileUtils.readFileToString(propOverrideFile, StandardCharsets.UTF_8);
        }
        String kylinConfigChecksum = DigestUtils.sha256Hex(propContent);
        int ret = KylinConfigChecksumCLI.execute(new String[] { kylinConfigChecksum });
        assertEquals(0, ret);
    }

    @Test
    public void testCorrectChecksum4PropOverrideExists() throws Exception {
        File kylinConfDir = KylinConfig.getKylinConfDir();
        File propFile = new File(kylinConfDir, KYLIN_CONF_PROPERTIES_FILE);
        File propOverrideFile = new File(kylinConfDir, KYLIN_CONF_PROPERTIES_FILE + ".override");
        boolean testAdd = false;
        if (!propOverrideFile.exists()) {
            testAdd = true;
            FileUtils.writeStringToFile(propOverrideFile, "a=1", StandardCharsets.UTF_8);
        }
        String propContent = FileUtils.readFileToString(propFile, StandardCharsets.UTF_8);
        propContent += FileUtils.readFileToString(propOverrideFile, StandardCharsets.UTF_8);
        String kylinConfigChecksum = DigestUtils.sha256Hex(propContent);
        int ret = KylinConfigChecksumCLI.execute(new String[] { kylinConfigChecksum });
        assertEquals(0, ret);
        if (testAdd) {
            FileUtils.forceDelete(propOverrideFile);
        }
    }

    @Test
    public void testErrorChecksum() {
        int ret = KylinConfigChecksumCLI.execute(new String[] { "1" });
        assertEquals(1, ret);
    }

    @Test
    public void testExceptionOccurred() {
        PowerMockito.mockStatic(DigestUtils.class);
        PowerMockito.when(DigestUtils.sha256Hex(anyString())).thenThrow(new RuntimeException());
        int ret = KylinConfigChecksumCLI.execute(new String[] { "1" });
        assertEquals(-1, ret);
    }
}
