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

import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.META_KEY_PROPERTIES_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.metadata.mapper.ProjectMapper;
import org.apache.kylin.common.persistence.metadata.mapper.TableInfoMapper;
import org.apache.kylin.common.persistence.resources.ModelRawResource;
import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.apache.kylin.common.persistence.resources.TableInfoRawResource;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.dynamic.sql.select.SelectDSLCompleter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.IllegalTransactionStateException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
class JdbcMetadataStoreTest {

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    ProjectRawResource prj0;
    DataSource dataSource;
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    public void setup() throws Exception {
        prj0 = new ProjectRawResource();
        prj0.setMetaKey("prj0");
        prj0.setName("p0");
        prj0.setUuid(UUID.randomUUID().toString());
        prj0.setContent("{\"name\" : \"p0\"}".getBytes(DEFAULT_CHARSET));
        prj0.setTs(System.currentTimeMillis());
        prj0.setMvcc(0L);
        dataSource = JdbcDataSource.getDataSource(JdbcUtil.datasourceParameters(getTestConfig().getMetadataUrl()));
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Test
    void testBasic() throws Exception {
        final String uuid2 = UUID.randomUUID().toString();
        byte[] bytes2 = ("{ \"uuid\" : \"" + uuid2
                + "\",\"meta_key\" : \"db.abc2\",\"name\" : \"abc2\", \"project\" : \"pj1\"}")
                .getBytes(DEFAULT_CHARSET);
        UnitOfWork.doInTransactionWithRetry(() -> {
            UnitOfWork.get().getCopyForWriteItems().add("TABLE_INFO/db.abc");
            UnitOfWork.get().getCopyForWriteItems().add("TABLE_INFO/db.abc2");
            UnitOfWork.get().getCopyForWriteItems().add("TABLE_INFO/db.abc3");
            UnitOfWork.get().getCopyForWriteItems().add("TABLE_INFO/db.abc4");
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("TABLE_INFO/db.abc", ByteSource.wrap(
                    ("{ \"uuid\" : \"ffffffff-ffff-ffff-ffff-ffffffffffff\",\"meta_key\" : \"db.abc\",\"name\" : " +
                             "\"abc\", \"project\" : \"pj1\"}")
                            .getBytes(DEFAULT_CHARSET)),
                    -1);

            RawResource rawResource = store.checkAndPutResource("TABLE_INFO/db.abc2", ByteSource.wrap(bytes2), -1);
            Assertions.assertEquals(uuid2, rawResource.getUuid());
            Assertions.assertEquals("pj1", rawResource.getProject());
            Assertions.assertEquals("db.abc2", rawResource.getMetaKey());

            String uuid = UUID.randomUUID().toString();
            store.checkAndPutResource("TABLE_INFO/db.abc3",
                    ByteSource.wrap(("{ \"uuid\" : \"" + uuid
                            + "\",\"meta_key\" : \"db.abc3\",\"name\" : \"abc3\", \"project\" : \"pj1\"}")
                            .getBytes(DEFAULT_CHARSET)),
                    -1);
            store.checkAndPutResource("TABLE_INFO/db.abc3",
                    ByteSource.wrap(("{ \"uuid\" : \"" + uuid
                            + "\",\"meta_key\" : \"db.abc3\",\"name\" : \"abc3\", \"project\" : \"pj1\"}")
                            .getBytes(DEFAULT_CHARSET)),
                    0);

            uuid = UUID.randomUUID().toString();
            store.checkAndPutResource("TABLE_INFO/db.abc4",
                    ByteSource.wrap(("{ \"uuid\" : \"" + uuid
                            + "\",\"meta_key\" : \"db.abc4\",\"name\" : \"abc4\", \"project\" : \"pj1\"}")
                            .getBytes(DEFAULT_CHARSET)),
                    1000L, -1);
            store.deleteResource("TABLE_INFO/db.abc");
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        try {
            SqlSessionFactory sqlSessionFactory = MetadataMapperFactory.getSqlSessionFactory(dataSource);
            try (SqlSession session = sqlSessionFactory.openSession()) {
                val mapper = session.getMapper(TableInfoMapper.class);
                List<TableInfoRawResource> all = mapper.select(SelectDSLCompleter.allRows());
                Assertions.assertEquals(3, all.size());
                for (TableInfoRawResource resource : all) {
                    if (resource.getMetaKey().equals(UnitOfWork.GLOBAL_UNIT)) {
                        Assertions.assertEquals(0, resource.getMvcc());
                        // Check the mapper can get the decompressed content
                        Assertions.assertArrayEquals(bytes2, resource.getContent());
                    }
                    if (resource.getMetaKey().equals("db.abc3")) {
                        Assertions.assertEquals(1, resource.getMvcc());
                    }
                    if (resource.getMetaKey().equals("db.abc4")) {
                        Assertions.assertEquals(1000L, resource.getTs());
                    }
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReload() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            UnitOfWork.get().getCopyForWriteItems().add("TABLE_INFO/db.abc2");
            String uuid = UUID.randomUUID().toString();
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("TABLE_INFO/db.abc2",
                    ByteSource.wrap(("{ \"uuid\" : \"" + uuid
                            + "\",\"meta_key\" : \"db.abc2\",\"name\" : \"abc2\", \"project\" : \"pj1\"}")
                            .getBytes(DEFAULT_CHARSET)),
                    -1);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        try {
            SqlSessionFactory sqlSessionFactory = MetadataMapperFactory.getSqlSessionFactory(dataSource);
            try (SqlSession session = sqlSessionFactory.openSession()) {
                val mapper = session.getMapper(TableInfoMapper.class);
                List<TableInfoRawResource> all = mapper.select(SelectDSLCompleter.allRows());
                Assertions.assertEquals(1, all.size());
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        ResourceStore systemStore = ResourceStore.getKylinMetaStore(getTestConfig());
        systemStore.reload();
        int size = systemStore.listResourcesRecursively(MetadataType.TABLE_INFO.name()).size();
        Assertions.assertEquals(1, size);

    }

    @Test
    void testBatchInsert() throws Exception {
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        SqlSessionFactory sqlSessionFactory = MetadataMapperFactory.getSqlSessionFactory(dataSource);
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(ProjectMapper.class);
            ProjectRawResource prj1 = new ProjectRawResource();
            prj1.setMetaKey("prj1");
            prj1.setName(UnitOfWork.GLOBAL_UNIT);
            prj1.setUuid(UUID.randomUUID().toString());
            prj1.setContent("{\"name\" : \"p1\"}".getBytes(DEFAULT_CHARSET));
            prj1.setTs(System.currentTimeMillis());
            prj1.setMvcc(0L);
            int count = mapper.insertMultiple(Arrays.asList(prj0, prj1));
            Assertions.assertEquals(2, count);
            List<ModelRawResource> collect = IntStream.range(0, 2048).mapToObj(i -> {
                String uuid = UUID.randomUUID().toString();
                ModelRawResource md = new ModelRawResource();
                md.setMetaKey(uuid);
                md.setAlias("m" + i);
                md.setUuid(uuid);
                md.setProject("p" + (i / 1024));
                md.setContent(("{\"uuid\" : \"" + uuid + "\",\"alias\" : \"m" + i + "\"}").getBytes(DEFAULT_CHARSET));
                md.setTs(System.currentTimeMillis());
                md.setMvcc(0L);
                return md;
            }).collect(Collectors.toList());

            UnitOfWork.doInTransactionWithRetry(() -> {
                ResourceStore kylinMetaStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                MetadataStore metadataStore = kylinMetaStore.getMetadataStore();
                log.info("start to batch insert...");
                Date date = new Date();
                collect.forEach(md -> metadataStore.save(md.getMetaType(), md));
                log.info("batch insert cost {} ms", System.currentTimeMillis() - date.getTime());
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        }
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        resourceStore.reload();
        Assertions.assertEquals(2048, resourceStore.listResourcesRecursively(MetadataType.MODEL.name()).size());
    }

    @Test
    void testMvccConflict() {
        String uuid = UUID.randomUUID().toString();
        String resourcePath = MetadataType.mergeKeyWithType("abc", MetadataType.PROJECT);
        ByteSource entityBytes = ByteSource
                .wrap(("{ \"uuid\" : \"" + uuid + "\",\"name\" : \"abc\"}").getBytes(DEFAULT_CHARSET));
        UnitOfWork.doInTransactionWithRetry(() -> {
            UnitOfWork.get().getCopyForWriteItems().add(resourcePath);
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource(resourcePath, entityBytes, -1);
            store.checkAndPutResource(resourcePath, entityBytes, 0);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        String identifier = getTestConfig().getMetadataUrl().getIdentifier();
        jdbcTemplate.update("update " + identifier + "_project set mvcc = 10");

        UnitOfWork.doInTransactionWithRetry(() -> {
            UnitOfWork.get().getCopyForWriteItems().add(resourcePath);
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            Assertions.assertThrows(PersistenceException.class,
                    () -> store.checkAndPutResource(resourcePath, entityBytes, 1));
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    void testUpdateTheDeletedResource() {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        ReflectionTestUtils.setField(metadataStore, "isUT", false);
        ProjectRawResource prj1 = new ProjectRawResource();
        prj1.setMetaKey("prj0");
        prj1.setName("p0");
        prj1.setUuid(UUID.randomUUID().toString());
        prj1.setContent("{\"name\" : \"p0\"}".getBytes(DEFAULT_CHARSET));
        prj1.setTs(System.currentTimeMillis());
        prj1.setMvcc(4);
        Assertions.assertThrows(TransactionException.class, () -> UnitOfWork.doInTransactionWithRetry(() -> {
            metadataStore.save(MetadataType.PROJECT, prj1);
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1));
    }

    @OverwriteProp.OverwriteProps({ //
            @OverwriteProp(key = "kylin.metadata.compress.enabled", value = "false"), //
            @OverwriteProp(key = "kylin.server.port", value = "8081")//
    })
    @Test
    void testBatchUpdateWithMetadataCompressDisable() throws Exception {
        val metadataStore = ResourceStore.getKylinMetaStore(getTestConfig()).getMetadataStore();

        RawResource prj1 = new ProjectRawResource();
        prj1.setMetaKey("prj1");
        prj1.setUuid(UUID.randomUUID().toString());
        prj1.setContent("{\"name\" : \"prj1\"}".getBytes(DEFAULT_CHARSET));
        prj1.setTs(System.currentTimeMillis());
        prj1.setMvcc(-1);
        System.out.println("prj1 uuid is:" + prj1.getUuid());

        List<Event> events = Collections
                .singletonList(new ResourceCreateOrUpdateEvent(prj1.generateKeyWithType(), prj1));
        val unitMessages = new UnitMessages(events);
        UnitOfWork.doInTransactionWithRetry(() -> {
            metadataStore.save(prj1.getMetaType(), prj1);
            metadataStore.getAuditLogStore().save(unitMessages);
            return null;
        }, prj1.getMetaKey());
        RawResourceFilter filter = RawResourceFilter.equalFilter(META_KEY_PROPERTIES_NAME, prj1.getMetaKey());
        String content = new String(ByteSource
                .wrap(metadataStore.get(MetadataType.PROJECT, filter, true, true).get(0).getContent()).read());
        Assertions.assertEquals("{\"name\" : \"prj1\"}", content);

        // Test get with provideSelections
        List<String> provideSelections = new java.util.ArrayList<>();
        provideSelections.add(META_KEY_PROPERTIES_NAME);
        List<ProjectRawResource> list = ((JdbcMetadataStore) metadataStore).get(MetadataType.PROJECT, filter, true,
                true, provideSelections);
        ProjectRawResource projectRawResource = list.get(0);
        // It will only include the metaKey and content field
        Assertions.assertNull(projectRawResource.getName());
        Assertions.assertNull(projectRawResource.getTs());
        Assertions.assertEquals("_global", projectRawResource.getProject());
        Assertions.assertEquals("prj1", projectRawResource.getMetaKey());
        Assertions.assertEquals("{\"name\" : \"prj1\"}",
                new String(ByteSource.wrap(projectRawResource.getContent()).read()));

        String identifier = getTestConfig().getMetadataUrl().getIdentifier();
        byte[] contents = jdbcTemplate.queryForObject(
                "select content from " + identifier + "_project where meta_key = 'prj1'",
                (rs, rowNum) -> rs.getBytes(1));

        Assertions.assertFalse(CompressionUtils.isCompressed(contents));

        byte[] auditLogContents = jdbcTemplate.queryForObject(
                "select meta_content from " + identifier + JdbcAuditLogStore.AUDIT_LOG_SUFFIX + " where meta_key = 'PROJECT/prj1'",
                (rs, rowNum) -> rs.getBytes(1));

        Assertions.assertFalse(CompressionUtils.isCompressed(auditLogContents));
    }

    @Test
    void testGetResourceWithoutContent() {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        Assertions.assertInstanceOf(JdbcMetadataStore.class, metadataStore);
        metadataStore.save(MetadataType.PROJECT, prj0);
        List<ProjectRawResource> projects = metadataStore.get(MetadataType.PROJECT,
                RawResourceFilter.equalFilter("name", prj0.getName()), false, false);
        Assertions.assertNotNull(prj0.getContent());
        Assertions.assertEquals(1, projects.size());
        Assertions.assertNull(projects.get(0).getContent());
    }

    @Test
    void testDump() throws IOException, ExecutionException, InterruptedException {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        InMemResourceStore rs = new InMemResourceStore(getTestConfig());
        RawResourceFilter emptyFilter = new RawResourceFilter();

        metadataStore.dump(rs);
        Assertions.assertEquals(0, metadataStore.get(MetadataType.PROJECT, emptyFilter, false, false).size());
        rs.putResourceWithoutCheck(prj0.generateKeyWithType(), prj0.getByteSource(), prj0.getTs(), prj0.getMvcc());
        metadataStore.dump(rs);
        Assertions.assertEquals(1, metadataStore.get(MetadataType.PROJECT, emptyFilter, false, false).size());
    }

    @Test
    void testUploadFromFile() throws IOException {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        File tmpDir = Files.createTempDirectory("Metadata_").toFile();
        File prjFolder = new File(tmpDir, "PROJECT");
        File prjFile = new File(prjFolder, "prj0.json");
        prjFolder.deleteOnExit();
        prjFile.deleteOnExit();
        if (!(prjFolder.mkdirs() && prjFile.createNewFile())) {
            Assertions.fail("can not create file.");
        }
        RawResourceFilter emptyFilter = new RawResourceFilter();
        JsonUtil.writeValue(prjFile, prj0);

        Assertions.assertEquals(0, metadataStore.get(MetadataType.PROJECT, emptyFilter, false, false).size());
        metadataStore.uploadFromFile(tmpDir);
        Assertions.assertEquals(1, metadataStore.get(MetadataType.PROJECT, emptyFilter, false, false).size());
    }

    @Test
    void testDeleteNotExistResource() {
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        prj0.setContent(null);
        Assertions.assertEquals(-1, metadataStore.save(MetadataType.PROJECT, prj0));
    }

    @Test
    void testTransactionCheck() throws Exception {
        JdbcMetadataStore ms = (JdbcMetadataStore) ResourceStore.getKylinMetaStore(getTestConfig()).getMetadataStore();
        DataSourceTransactionManager manager = ms.getTransactionManager();
        try {
            JdbcUtil.withTransaction(manager, () -> {
                UnitOfWork.doInTransactionWithRetry(() -> true, "default");
                return true;
            });
            Assertions.fail("Expected an exception");
        } catch (Exception e) {
            Assertions.assertInstanceOf(TransactionException.class, e.getCause());
            Assertions.assertInstanceOf(IllegalTransactionStateException.class, e.getCause().getCause());
        }
    }

    KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }
}
