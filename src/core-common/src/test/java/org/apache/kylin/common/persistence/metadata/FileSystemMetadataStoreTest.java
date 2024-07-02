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

import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.EQUAL;
import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE;
import static org.apache.kylin.common.persistence.metadata.FileSystemFilterFactory.convertConditionsToFilter;
import static org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore.FILE_SCHEME;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.resources.ModelRawResource;
import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;

/**
 * 27/12/2023 hellozepp(lisheng.zhanglin@163.com)
 */
public class FileSystemMetadataStoreTest extends NLocalFileMetadataTestCase {
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testPathFilter() {
        RawResourceFilter rawResourceFilter = new RawResourceFilter();
        rawResourceFilter.addConditions(new RawResourceFilter.Condition("metaKey",
                Collections.singletonList("DEFAULT.TEST_KYLIN_FACT"), EQUAL));
        var context = convertConditionsToFilter(rawResourceFilter, MetadataType.TABLE_INFO);
        String resPath = context.getResPath();
        var regex = context.getRegex();
        var jsonFilter = context.getRawResourceFilter();
        boolean isWholePath = context.isWholePath();
        Assert.assertEquals("TABLE_INFO/DEFAULT.TEST_KYLIN_FACT.json", resPath);
        Assert.assertNull(regex);
        Assertions.assertThat(jsonFilter.getConditions().isEmpty()).isTrue();
        Assert.assertTrue(isWholePath);

        // '>' and '<' Operator
        rawResourceFilter = RawResourceFilter.simpleFilter(RawResourceFilter.Operator.GT, "createTime",
                "1604288097900");
        context = convertConditionsToFilter(rawResourceFilter, MetadataType.TABLE_INFO);
        resPath = context.getResPath();
        regex = context.getRegex();
        jsonFilter = context.getRawResourceFilter();
        isWholePath = context.isWholePath();
        Assert.assertEquals("TABLE_INFO", resPath);
        Assert.assertNull(regex);
        Assertions.assertThat(jsonFilter.getConditions().size()).isSameAs(1);
        Assert.assertFalse(isWholePath);
    }

    @Test
    public void testRegexTablePathFilter() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            UnitOfWork.get().getCopyForWriteItems().add("TABLE_INFO/pj1.db.abc2");
            val store1 = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            String uuid = UUID.randomUUID().toString();
            store1.checkAndPutResource("TABLE_INFO/pj1.db.abc2", ByteSource.wrap(("{ \"uuid\" : \"" + uuid
                    + "\",\"meta_key\" : \"pj1.db.abc2\",\"name\" : \"abc2\", \"project\" : \"pj1\",\"database\""
                    + " : \"db\", \"id\" : \"20002\" }").getBytes(DEFAULT_CHARSET)), -1);
            return 0;
        }, "pj1");

        UnitOfWork.doInTransactionWithRetry(() -> {
            val store1 = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

            RawResourceFilter filter = new RawResourceFilter();
            filter.addConditions(new RawResourceFilter.Condition("name", Collections.singletonList("abc2"), EQUAL));
            List<RawResource> tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter);
            checkTable(tableInfo);

            RawResourceFilter filter2 = new RawResourceFilter();
            filter2.addConditions(new RawResourceFilter.Condition("name", Collections.singletonList("ABC2"),
                    RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE));
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter2);
            checkTable(tableInfo);

            RawResourceFilter filter3 = new RawResourceFilter();
            filter3.addConditions(new RawResourceFilter.Condition("name", Collections.singletonList("B1C"),
                    RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE));
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter3);
            Assert.assertEquals(0, tableInfo.size());

            RawResourceFilter filter4 = new RawResourceFilter();
            filter4.addConditions("dbName", Arrays.asList("db.xxx", "db"), RawResourceFilter.Operator.IN)
                    .addConditions("name", Arrays.asList("abc2", "xxx"), RawResourceFilter.Operator.IN);
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter4);
            checkTable(tableInfo);

            // '>' and '<' Operator
            RawResourceFilter filter5 = RawResourceFilter.simpleFilter(RawResourceFilter.Operator.GT, "createTime",
                    "1604288097900");
            store1.getMatchedResourcesWithoutContent("HISTORY_SOURCE_USAGE", filter5);

            RawResourceFilter filter6 = new RawResourceFilter();
            filter6.addConditions("dbName", Collections.singletonList("D"),
                    RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE).addConditions("name",
                            Collections.singletonList("c2"), RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE);
            var context = convertConditionsToFilter(filter6, MetadataType.TABLE_INFO);
            Assert.assertEquals("TABLE_INFO", context.getResPath());
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter6);
            checkTable(tableInfo);

            RawResourceFilter filter7 = new RawResourceFilter();
            filter7.addConditions(
                    new RawResourceFilter.Condition("metaKey", Collections.singletonList("pj1.db.abc2"), EQUAL));
            context = convertConditionsToFilter(filter7, MetadataType.TABLE_INFO);
            Assert.assertEquals("TABLE_INFO/pj1.db.abc2.json", context.getResPath());
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter7);
            checkTable(tableInfo);

            RawResourceFilter filter8 = new RawResourceFilter();
            filter8.addConditions(new RawResourceFilter.Condition("metaKey", Collections.singletonList("pj1.DB"),
                    RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE));
            context = convertConditionsToFilter(filter8, MetadataType.TABLE_INFO);
            Assert.assertEquals("(?i).*\\Qpj1.DB\\E.*", context.getRegex());
            Assert.assertEquals("TABLE_INFO", context.getResPath());
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter8);
            checkTable(tableInfo);

            // case insensitive
            RawResourceFilter filter9 = new RawResourceFilter();
            filter9.addConditions(new RawResourceFilter.Condition("metaKey", Collections.singletonList("pj1.db.ABC2"),
                    RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE));
            context = convertConditionsToFilter(filter9, MetadataType.TABLE_INFO);
            Assert.assertEquals("(?i)\\Qpj1.db.ABC2.json\\E", context.getRegex());
            Assert.assertEquals("TABLE_INFO", context.getResPath());
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter9);
            checkTable(tableInfo);

            // IN Operator
            RawResourceFilter filter10 = new RawResourceFilter();
            filter10.addConditions(new RawResourceFilter.Condition("metaKey",
                    Arrays.asList("pj1.db.abc2", "123", "321"), RawResourceFilter.Operator.IN));
            context = convertConditionsToFilter(filter10, MetadataType.TABLE_INFO);
            Assert.assertEquals("\\Qpj1.db.abc2.json\\E|\\Q123.json\\E|\\Q321.json\\E", context.getRegex());
            Assert.assertEquals("TABLE_INFO", context.getResPath());
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter10);
            checkTable(tableInfo);

            // in case of empty values
            RawResourceFilter filter11 = new RawResourceFilter();
            filter11.addConditions(
                    new RawResourceFilter.Condition("metaKey", Collections.emptyList(), RawResourceFilter.Operator.IN));
            context = convertConditionsToFilter(filter11, MetadataType.TABLE_INFO);
            Assert.assertEquals("", context.getRegex());
            Assert.assertEquals("TABLE_INFO", context.getResPath());
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter11);
            Assert.assertEquals(0, tableInfo.size());

            // in case of empty values
            RawResourceFilter filter12 = new RawResourceFilter();
            filter12.addConditions(new RawResourceFilter.Condition("metaKey", Arrays.asList("pj1.db.", "123"),
                    RawResourceFilter.Operator.IN));
            context = convertConditionsToFilter(filter12, MetadataType.TABLE_INFO);
            Assert.assertEquals("\\Qpj1.db..json\\E|\\Q123.json\\E", context.getRegex());
            Assert.assertEquals("TABLE_INFO", context.getResPath());
            tableInfo = store1.getMatchedResourcesWithoutContent("TABLE_INFO", filter12);
            Assert.assertEquals(0, tableInfo.size());
            return 0;
        }, "pj1");
    }

    void checkTable(List<RawResource> tableInfo) {
        Assert.assertEquals(1, tableInfo.size());
        Assert.assertEquals("pj1.db.abc2", tableInfo.get(0).getMetaKey());
    }

    @Test
    public void testGetByModelPathFilter() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store1 = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            RawResourceFilter rawResourceFilter = new RawResourceFilter();
            rawResourceFilter.addConditions(new RawResourceFilter.Condition("alias",
                    Collections.singletonList("test_MODEL"), RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE));
            rawResourceFilter.addConditions(new RawResourceFilter.Condition("metaKey",
                    Collections.singletonList("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da"), EQUAL));
            List<RawResource> tableInfo = store1.getMatchedResourcesWithoutContent("MODEL", rawResourceFilter);
            Assert.assertEquals(1, tableInfo.size());
            return 0;
        }, "default");
    }

    @Test
    public void testGetNotSupportColByModelPathFilter() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            RawResource resource = store.getResource("MODEL/0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");
            Assert.assertEquals("cc_test", resource.getProject());
            Assert.assertEquals("test_model", ((ModelRawResource) resource).getAlias());

            RawResourceFilter rawResourceFilter = new RawResourceFilter();
            // Only Support outer
            rawResourceFilter.addConditions(new RawResourceFilter.Condition("factTable",
                    Collections.singletonList("SSB.LINEORDER"), RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE));
            rawResourceFilter.addConditions(new RawResourceFilter.Condition("metaKey",
                    Collections.singletonList("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da"), EQUAL));
            thrown.expect(TransactionException.class);
            store.getMatchedResourcesWithoutContent("MODEL", rawResourceFilter);

            FileSystemMetadataStore metadataStore = (FileSystemMetadataStore) store.getMetadataStore();
            NavigableSet<String> set = metadataStore.listAll();
            Assert.assertTrue(set.contains("MODEL/9852f045-040c-41aa-acf5-597d9507b66a"));
            return 0;
        }, "default");
    }

    @Test
    public void testValidFileSchema() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl("/home/kylin/demo");
        FileSystemMetadataStore metadataStore = new FileSystemMetadataStore(config);
        checkMetadataStore("/home/kylin/demo", metadataStore);

        config.setMetadataUrl("file:///home/kylin/demo");
        metadataStore = new FileSystemMetadataStore(config);
        checkMetadataStore("/home/kylin/demo", metadataStore);

        config.setMetadataUrl("file:///home/kylin/demo@file");
        metadataStore = new FileSystemMetadataStore(config);
        checkMetadataStore("/home/kylin/demo", metadataStore);

        config.setMetadataUrl("/home/kylin/demo@file");
        metadataStore = new FileSystemMetadataStore(config);
        checkMetadataStore("/home/kylin/demo", metadataStore);

        String currentPath = Paths.get("").toFile().getAbsolutePath();

        config.setMetadataUrl("./demo");
        metadataStore = new FileSystemMetadataStore(config);
        checkMetadataStore(currentPath + "/demo", metadataStore);

        config.setMetadataUrl("file");
        metadataStore = new FileSystemMetadataStore(config);
        checkMetadataStore(currentPath + "/file", metadataStore);

        config.setMetadataUrl("meta@hdfs");
        metadataStore = new FileSystemMetadataStore(config);
        String targetPath = HadoopUtil.getBackupFolder(config).substring("file://".length()) + "/backup_0";
        checkMetadataStore(targetPath, metadataStore);
    }

    private void checkMetadataStore(String path, FileSystemMetadataStore metadataStore) {
        // Local file is used in UT, so the file schema is always FILE_SCHEME
        Assert.assertEquals(FILE_SCHEME, metadataStore.fs.getScheme());
        Assert.assertEquals(path, metadataStore.getRootPath().toUri().getPath());
    }

    @Test
    public void testNotSupportSchema() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl("/kylin/demo@Unknown");
        Assert.assertThrows(IllegalStateException.class, () -> new FileSystemMetadataStore(config));
    }

    @Test
    public void testListAllInMetadataStoreIsNotAllowed() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        MetadataStore store = ResourceStore.getKylinMetaStore(config).getMetadataStore();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> store.get(MetadataType.ALL, new RawResourceFilter(), false, false));
    }

    @Test
    public void testGetFromZip() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl("test,zip=1");
        FileSystemMetadataStore store = Mockito.spy(new FileSystemMetadataStore(config));

        ProjectRawResource prj = new ProjectRawResource();
        prj.setName("p1");
        prj.setMetaKey("p1");
        prj.setContent("abc".getBytes());
        Map<String, RawResource> mockedResources = new HashMap<>();
        mockedResources.put("PROJECT/p1.json", prj);
        Mockito.doReturn(mockedResources).when(store).getCompressedFiles();

        //test filter with accurate metaKey
        testAllBranches(store, prj, EQUAL);
        //test filter without accurate metaKey
        testAllBranches(store, prj, EQUAL_CASE_INSENSITIVE);
    }

    private void testAllBranches(FileSystemMetadataStore store, ProjectRawResource prj,
            RawResourceFilter.Operator operator) {
        RawResourceFilter filter = new RawResourceFilter();
        filter.addConditions("metaKey", Collections.singletonList("p1"), operator);
        List<RawResource> results = store.get(MetadataType.PROJECT, filter, false, true);
        Assert.assertTrue(results.size() == 1 && results.get(0) == prj);
        results = store.get(MetadataType.PROJECT, filter, false, false);
        Assert.assertTrue(results.size() == 1 && results.get(0) == prj && results.get(0).getContent() == null);

        filter = new RawResourceFilter();
        filter.addConditions("metaKey", Collections.singletonList("p2"), operator);
        results = store.get(MetadataType.PROJECT, filter, false, true);
        Assert.assertTrue(results.isEmpty());

        filter = new RawResourceFilter();
        filter.addConditions("metaKey", Collections.singletonList("p1"), operator);
        filter.addConditions("name", Collections.singletonList("p2"), EQUAL);
        results = store.get(MetadataType.PROJECT, filter, false, true);
        Assert.assertTrue(results.isEmpty());

        filter = new RawResourceFilter();
        filter.addConditions("metaKey", Collections.singletonList("p2"), operator);
        filter.addConditions("name", Collections.singletonList("p2"), EQUAL);
        results = store.get(MetadataType.PROJECT, filter, false, true);
        Assert.assertTrue(results.isEmpty());
    }

}
