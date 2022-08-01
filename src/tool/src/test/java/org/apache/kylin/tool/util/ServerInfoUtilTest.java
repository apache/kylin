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
package org.apache.kylin.tool.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class ServerInfoUtilTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testGetKylinClientInformation() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        overwriteSystemProp("KYLIN_HOME", mainDir.getAbsolutePath());

        List<File> clearFileList = new ArrayList<>();
        String sha1 = "6a38664fe087f7f466ec4ad9ac9dc28415d99e52@KAP\nBuild with MANUAL at 2019-08-31 20:02:22";
        File sha1File = new File(KylinConfig.getKylinHome(), "commit_SHA1");
        if (!sha1File.exists()) {
            FileUtils.writeStringToFile(sha1File, sha1);
            clearFileList.add(sha1File);
        }

        String version = "Kyligence Enterprise 4.0.0-SNAPSHOT";
        File versionFile = new File(KylinConfig.getKylinHome(), "VERSION");
        if (!versionFile.exists()) {
            FileUtils.writeStringToFile(versionFile, version);
            clearFileList.add(versionFile);
        }

        String buf = ServerInfoUtil.getKylinClientInformation();

        for (File file : clearFileList) {
            FileUtils.deleteQuietly(file);
        }

        Assert.assertTrue(buf.contains("commit:" + sha1.replace('\n', ';')));
        Assert.assertTrue(buf.contains("kap.version:" + version));
    }
}
