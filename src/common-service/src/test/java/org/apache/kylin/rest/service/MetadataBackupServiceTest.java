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
package org.apache.kylin.rest.service;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.tool.HDFSMetadataTool;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;

public class MetadataBackupServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private MetadataBackupService metadataBackupService = Mockito.spy(new MetadataBackupService());

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void init() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBackup() throws Exception {
        val junitFolder = temporaryFolder.getRoot();

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", junitFolder.getAbsolutePath());
        kylinConfig.setMetadataUrl("metadata_backup_ut_test");
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);

        if (!resourceStore.exists("/UUID")) {
            resourceStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
        }

        //1.assert there is no metadata dir in root dir before backup,the root dir is junitFolder.getAbsolutePath()
        val rootPath = new Path(kylinConfig.getHdfsWorkingDirectory()).getParent();
        val rootFS = HadoopUtil.getWorkingFileSystem();
        Assertions.assertThat(rootFS.listStatus(rootPath)).isEmpty();

        //2.execute backup()
        var backupFolder = metadataBackupService.backupAll();

        //3.assert there is a metadata dir in root metadata dir after backup,the metadata dir location is junitFolder.getAbsolutePath()/metadata_backup_ut_test/backup/LocalDateTime/metadata
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");
        val rootMetadataFS = HadoopUtil.getWorkingFileSystem();
        Assertions.assertThat(rootMetadataFS.listStatus(rootMetadataPath)).hasSize(1);

        val rootMetadataChildrenPath = rootMetadataFS.listStatus(rootMetadataPath)[0].getPath();
        Assertions.assertThat(rootMetadataChildrenPath.toUri().toString()).isEqualTo(backupFolder.getFirst());
        Assertions.assertThat(rootMetadataChildrenPath.getName()).isEqualTo(backupFolder.getSecond());
        Assertions.assertThat(rootMetadataFS.listStatus(rootMetadataChildrenPath)).hasSize(2).contains(
                rootMetadataFS.getFileStatus(new Path(rootMetadataChildrenPath.toString() + File.separator + "UUID")),
                rootMetadataFS
                        .getFileStatus(new Path(rootMetadataChildrenPath.toString() + File.separator + "_image")));

    }

    @Test
    public void testCleanBeforeBackup() throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", temporaryFolder.getRoot().getAbsolutePath());
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");

        val fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(rootMetadataPath);

        int metadataBackupCountThreshold = kylinConfig.getMetadataBackupCountThreshold();
        for (int i = 0; i < metadataBackupCountThreshold - 1; i++) {
            fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + i));
        }
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(6);

        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + (metadataBackupCountThreshold - 1)));
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(7);

        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + metadataBackupCountThreshold));
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(7);

        kylinConfig.setProperty("kylin.metadata.backup-count-threshold", "3");
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(2);
    }
}
