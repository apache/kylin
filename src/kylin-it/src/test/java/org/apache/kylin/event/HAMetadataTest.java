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

package org.apache.kylin.event;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.awaitility.Awaitility.await;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.tool.MetadataTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HAMetadataTest extends NLocalFileMetadataTestCase {

    private KylinConfig queryKylinConfig;
    private ResourceStore queryResourceStore;
    private final Charset charset = StandardCharsets.UTF_8;

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("kylin.metadata.audit-log.catchup-interval", "1s");
        createTestMetadata();
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "false");
        getTestConfig().setMetadataUrl("test" + System.currentTimeMillis()
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
            return null;
        }, "");
        queryKylinConfig = KylinConfig.createKylinConfig(getTestConfig());
        val auditLogStore = new JdbcAuditLogStore(queryKylinConfig);
        queryKylinConfig.setMetadataUrl("test@hdfs");
        queryResourceStore = ResourceStore.getKylinMetaStore(queryKylinConfig);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
    }

    @After
    public void tearDown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
        queryResourceStore.close();
        ((JdbcAuditLogStore) queryResourceStore.getAuditLogStore()).forceClose();
    }

    @Test
    public void testMetadataCatchup_EmptyBackup() throws InterruptedException {
        queryResourceStore.catchup();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteSource.wrap("path1".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path2", ByteSource.wrap("path2".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3", ByteSource.wrap("path3".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path4", ByteSource.wrap("path4".getBytes(charset)), -1);
            return 0;
        }, "p0");
        await().atMost(3, TimeUnit.SECONDS).until(() -> 5 == queryResourceStore.listResourcesRecursively("/").size());
    }

    @Test
    public void testMetadataCatchupWithBackup() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteSource.wrap("path1".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path2", ByteSource.wrap("path2".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3", ByteSource.wrap("path3".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path4", ByteSource.wrap("path4".getBytes(charset)), -1);
            return 0;
        }, "p0");
        String[] args = new String[] { "-backup", "-dir", HadoopUtil.getBackupFolder(getTestConfig()) };
        val metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        queryResourceStore.catchup();
        Assert.assertEquals(5, queryResourceStore.listResourcesRecursively("/").size());

        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteSource.wrap("path1".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path2", ByteSource.wrap("path2".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path3", ByteSource.wrap("path3".getBytes(charset)), 0);
            resourceStore.deleteResource("/p0/path4");
            resourceStore.checkAndPutResource("/p0/path5", ByteSource.wrap("path5".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path6", ByteSource.wrap("path6".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path7", ByteSource.wrap("path7".getBytes(charset)), -1);
            return 0;
        }, "p0");

        await().atMost(3, TimeUnit.SECONDS).until(() -> 7 == queryResourceStore.listResourcesRecursively("/").size());
        String table = getTestConfig().getMetadataUrl().getIdentifier() + "_audit_log";
        val auditCount = getJdbcTemplate().queryForObject(String.format(Locale.ROOT, "select count(*) from %s", table),
                Long.class);
        Assert.assertEquals(12L, auditCount.longValue());
    }

    @Ignore("unstable in daily ut")
    @Test
    public void testMetadata_RemoveAuditLog_Restore() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/_global/project/p0.json", ByteSource
                    .wrap("{  \"uuid\": \"1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b\"}".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path1.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path2.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path4.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 1 }".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path4.json",
                    ByteSource.wrap("{ \"mvcc\": 1 }".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 2 }".getBytes(charset)), 1);
            resourceStore.checkAndPutResource("/p0/path4.json",
                    ByteSource.wrap("{ \"mvcc\": 2 }".getBytes(charset)), 1);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 3 }".getBytes(charset)), 2);
            return 0;
        }, "p0");
        String table = getTestConfig().getMetadataUrl().getIdentifier() + "_audit_log";
        getJdbcTemplate().update(String.format(Locale.ROOT, "delete from %s where id=7", table));
        try {
            queryResourceStore.catchup();
            Assert.fail();
        } catch (Exception e) {
            queryResourceStore.close();
            ((JdbcAuditLogStore) queryResourceStore.getAuditLogStore()).forceClose();
        }
        await().pollDelay(1000, TimeUnit.MILLISECONDS).until(() -> true);
        String[] args = new String[] { "-backup", "-dir", HadoopUtil.getBackupFolder(getTestConfig()) };
        MetadataTool metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        await().pollDelay(1000, TimeUnit.MILLISECONDS).until(() -> true);
        val path = HadoopUtil.getBackupFolder(getTestConfig());
        val fs = HadoopUtil.getWorkingFileSystem();
        val rootPath = Stream.of(fs.listStatus(new Path(path)))
                .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                .orElse(new Path(path + "/backup_1/"));
        args = new String[] { "-restore", "-dir", rootPath.toString().substring(5), "--after-truncate" };
        metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        queryKylinConfig = KylinConfig.createKylinConfig(getTestConfig());
        val auditLogStore = new JdbcAuditLogStore(queryKylinConfig);
        queryKylinConfig.setMetadataUrl(getTestConfig().getMetadataUrl().getIdentifier() + "@hdfs");
        queryResourceStore = ResourceStore.getKylinMetaStore(queryKylinConfig);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
        queryResourceStore.catchup();

        Assert.assertEquals(7, queryResourceStore.listResourcesRecursively("/").size());
        val auditCount = getJdbcTemplate().queryForObject(String.format(Locale.ROOT, "select count(*) from %s", table),
                Long.class);
        Assert.assertEquals(15, auditCount.longValue());
        val imageDesc = JsonUtil.readValue(queryResourceStore.getResource("/_image").getByteSource().read(),
                ImageDesc.class);
        Assert.assertEquals(16, imageDesc.getOffset().longValue());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
