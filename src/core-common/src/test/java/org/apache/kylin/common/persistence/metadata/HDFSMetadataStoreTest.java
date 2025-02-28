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

package org.apache.kylin.common.persistence.metadata;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetadataInfo(onlyProps = true)
class HDFSMetadataStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSMetadataStoreTest.class);

    @Test
    void testStartKEWithBackupDir() throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KylinConfig newKylinConfig = KylinConfig.createKylinConfig(kylinConfig);

        newKylinConfig.setMetadataUrl(kylinConfig.getMetadataUrl().getIdentifier() + "@hdfs");
        String hdfsWorkingDir = newKylinConfig.getHdfsWorkingDirectory();

        String backupFolder = HadoopUtil.getBackupFolder(newKylinConfig);
        Assert.assertEquals(hdfsWorkingDir + "_backup", backupFolder);

        // for empty backup folder
        FileSystemMetadataStore hdfsMetadataStore = new FileSystemMetadataStore(newKylinConfig);
        String rootPath = hdfsMetadataStore.getRootPath().toString();
        Assert.assertTrue(rootPath.endsWith("/backup_0"));

        FileSystem fileSystem = HadoopUtil.getFileSystem(hdfsWorkingDir);
        // add empty backup dir for old version KE
        fileSystem.mkdirs(new Path(backupFolder + "/1_backup"));
        FileSystemMetadataStore hdfsMetadataStore2 = new FileSystemMetadataStore(newKylinConfig);
        String rootPath2 = hdfsMetadataStore2.getRootPath().toString();
        Assert.assertTrue(rootPath2.endsWith("/1_backup"));

        // add not empty backup dir for old version KE
        await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);
        fileSystem.mkdirs(new Path(backupFolder + "/2_backup"));
        fileSystem.createNewFile(new Path(backupFolder + "/2_backup", "metadata.zip"));
        FileSystemMetadataStore hdfsMetadataStore3 = new FileSystemMetadataStore(newKylinConfig);
        String rootPath3 = hdfsMetadataStore3.getRootPath().toString();
        Assert.assertTrue(rootPath3.endsWith("/2_backup"));

        // add backup dir for new version KE
        await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);
        fileSystem.mkdirs(new Path(backupFolder + "/3_backup"));
        fileSystem.mkdirs(new Path(backupFolder + "/3_backup/core_meta"));
        FileSystemMetadataStore hdfsMetadataStore4 = new FileSystemMetadataStore(newKylinConfig);
        String rootPath4 = hdfsMetadataStore4.getRootPath().toString();
        LOGGER.info("Root path for new version KE: {}", rootPath3);
        Assert.assertTrue(rootPath4.endsWith("/3_backup/core_meta"));
    }
}
