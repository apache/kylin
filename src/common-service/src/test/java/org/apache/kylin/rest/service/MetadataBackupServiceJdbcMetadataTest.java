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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStoreTool;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.io.Files;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import lombok.val;

@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
class MetadataBackupServiceJdbcMetadataTest {
    @InjectMocks
    private MetadataBackupService metadataBackupService = Mockito.spy(new MetadataBackupService());

    @Test
    @OverwriteProp(key = "kylin.metadata.audit-log.max-size", value = "20")
    void testAuditLogRotateWhenBackup(JdbcInfo info) throws Exception {
        val tempDir = Files.createTempDir();
        val kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", tempDir.getAbsolutePath());

        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        resourceStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assert.assertEquals(1, resourceStore.listResourcesRecursively("/").size());

        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        val table = url.getIdentifier() + "_audit_log";
        JdbcAuditLogStoreTool.prepareJdbcAuditLogStore(table, jdbcTemplate);
        long count = jdbcTemplate.queryForObject("select count(1) from " + table, Long.class);

        val rootPath = new Path(kylinConfig.getHdfsWorkingDirectory()).getParent();
        val rootFS = HadoopUtil.getWorkingFileSystem();
        Assertions.assertThat(rootFS.listStatus(rootPath)).isEmpty();

        metadataBackupService.backupAll();

        // make sure backup is successful
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");
        val rootMetadataFS = HadoopUtil.getWorkingFileSystem();
        Assert.assertEquals(1, rootMetadataFS.listStatus(rootMetadataPath).length);
        val path = rootMetadataFS.listStatus(rootMetadataPath)[0].getPath();
        Assert.assertEquals(2, rootMetadataFS.listStatus(path).length);
        FSDataInputStream fis = rootMetadataFS.open(new Path(path.toString() + File.separator + "_image"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
        String image = reader.readLine();
        ImageDesc imageDesc = JsonUtil.readValue(image, ImageDesc.class);
        Assertions.assertThat(imageDesc.getOffset()).isEqualTo(count);

        // assert delete audit_log
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            long newCount = jdbcTemplate.queryForObject("select count(1) from " + table, Long.class);
            return newCount == 20;
        });
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
    }
}
