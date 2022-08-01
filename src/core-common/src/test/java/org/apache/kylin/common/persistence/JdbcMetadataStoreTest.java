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
package org.apache.kylin.common.persistence;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.metadata.jdbc.RawResourceRowMapper;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;

@MetadataInfo(onlyProps = true)
@OverwriteProp(key = "kylin.metadata.url", value = "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=")
public class JdbcMetadataStoreTest {

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    @AfterEach
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
    }

    @Test
    public void testBasic() throws IOException {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc2", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc3", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc3", ByteSource.wrap("abc2".getBytes(DEFAULT_CHARSET)), 0);
            store.checkAndPutResource("/p1/abc4", ByteSource.wrap("abc2".getBytes(DEFAULT_CHARSET)), 1000L, -1);
            store.deleteResource("/p1/abc");
            return 0;
        }, "p1");
        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);
        val all = jdbcTemplate.query("select * from " + url.getIdentifier(), new RawResourceRowMapper());
        Assert.assertEquals(3, all.size());
        for (RawResource resource : all) {
            if (resource.getResPath().equals("/p1/abc2")) {
                Assert.assertEquals(0, resource.getMvcc());
            }
            if (resource.getResPath().equals("/p1/abc3")) {
                Assert.assertEquals(1, resource.getMvcc());
            }
            if (resource.getResPath().equals("/p1/abc4")) {
                Assert.assertEquals(1000L, resource.getTimestamp());
            }
        }
    }

    @Test
    public void testReload() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            return 0;
        }, "_global");

        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);
        val all = jdbcTemplate.query("select * from " + url.getIdentifier(), new RawResourceRowMapper());
        Assert.assertEquals(1, all.size());
        ResourceStore systemStore = ResourceStore.getKylinMetaStore(getTestConfig());
        systemStore.reload();
        Assert.assertEquals(1, systemStore.listResourcesRecursively("/").size());
    }

    @Test
    public void testPage() throws Exception {
        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);

        val tableName = url.getIdentifier();
        jdbcTemplate.execute(String.format(Locale.ROOT,
                "create table if not exists %s ( META_TABLE_KEY varchar(255) primary key, META_TABLE_CONTENT longblob, META_TABLE_TS bigint,  META_TABLE_MVCC bigint)",
                tableName));

        jdbcTemplate.batchUpdate(
                "insert into " + tableName
                        + " ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC ) values (?, ?, ?, ?)",
                Lists.newArrayList(
                        new Object[] { "/_global/project/p0.json", "project".getBytes(DEFAULT_CHARSET),
                                System.currentTimeMillis(), 0L },
                        new Object[] { "/_global/project/p1.json", "project".getBytes(DEFAULT_CHARSET),
                                System.currentTimeMillis(), 0L }));
        jdbcTemplate.batchUpdate(
                "insert into " + tableName
                        + " ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC ) values (?, ?, ?, ?)",
                IntStream.range(0, 2048)
                        .mapToObj(i -> new Object[] { "/p" + (i / 1024) + "/res" + i,
                                ("content" + i).getBytes(DEFAULT_CHARSET), System.currentTimeMillis(), 0L })
                        .collect(Collectors.toList()));

        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        Assert.assertEquals(1024, resourceStore.listResourcesRecursively("/p0").size());
        Assert.assertEquals(1024, resourceStore.listResourcesRecursively("/p1").size());
    }

    @Test
    @Disabled("for develop")
    public void testDuplicate() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), -1);
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), 0);
            return 0;
        }, "p1");

        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.update("update " + url.getIdentifier() + " set META_TABLE_MVCC = 10");

        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(DEFAULT_CHARSET)), 1);
            return 0;
        }, "p1");
    }

    @Test
    public void testBatchUpdate() throws Exception {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent(new RawResource("/p1/test",
                ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)), System.currentTimeMillis(), 0)));
        val unitMessages = new UnitMessages(events);
        UnitOfWork.doInTransactionWithRetry(() -> {
            metadataStore.batchUpdate(unitMessages, false, "/p1/test", -1);
            return null;
        }, "p1");

        String content = new String(metadataStore.load("/p1/test").getByteSource().read());
        Assert.assertEquals("test content", content);

        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        val tableName = url.getIdentifier();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);

        byte[] contents = jdbcTemplate.queryForObject(
                "select META_TABLE_CONTENT from " + tableName + " where META_TABLE_KEY = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertTrue(CompressionUtils.isCompressed(contents));
        byte[] gzip = "GZIP".getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(gzip, Arrays.copyOf(contents, gzip.length));
        Assert.assertArrayEquals("test content".getBytes(StandardCharsets.UTF_8),
                CompressionUtils.decompress(contents));

        byte[] auditLogContents = jdbcTemplate.queryForObject(
                "select meta_content from " + tableName + "_audit_log where meta_key = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertTrue(CompressionUtils.isCompressed(auditLogContents));
        Assert.assertArrayEquals(gzip, Arrays.copyOf(auditLogContents, gzip.length));
        Assert.assertArrayEquals("test content".getBytes(StandardCharsets.UTF_8),
                CompressionUtils.decompress(auditLogContents));
    }

    @OverwriteProp.OverwriteProps({ //
            @OverwriteProp(key = "kylin.metadata.compress.enabled", value = "false"), //
            @OverwriteProp(key = "kylin.server.port", value = "8081")//
    })
    @Test
    public void testBatchUpdateWithMetadataCompressDisable() throws Exception {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent(new RawResource("/p1/test",
                ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)), System.currentTimeMillis(), 0)));
        val unitMessages = new UnitMessages(events);
        UnitOfWork.doInTransactionWithRetry(() -> {
            metadataStore.batchUpdate(unitMessages, false, "/p1/test", -1);
            return null;
        }, "p1");

        String content = new String(metadataStore.load("/p1/test").getByteSource().read());
        Assert.assertEquals("test content", content);

        val dataSource = new DriverManagerDataSource();
        val url = getTestConfig().getMetadataUrl();
        val tableName = url.getIdentifier();
        dataSource.setUrl(url.getParameter("url"));
        dataSource.setDriverClassName(url.getParameter("driverClassName"));
        dataSource.setUsername(url.getParameter("username"));
        dataSource.setPassword(url.getParameter("password"));
        val jdbcTemplate = new JdbcTemplate(dataSource);

        byte[] contents = jdbcTemplate.queryForObject(
                "select META_TABLE_CONTENT from " + tableName + " where META_TABLE_KEY = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertFalse(CompressionUtils.isCompressed(contents));

        byte[] auditLogContents = jdbcTemplate.queryForObject(
                "select meta_content from " + tableName + "_audit_log where meta_key = '/p1/test'",
                (rs, rowNum) -> rs.getBytes(1));

        Assert.assertFalse(CompressionUtils.isCompressed(auditLogContents));
    }

    KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
