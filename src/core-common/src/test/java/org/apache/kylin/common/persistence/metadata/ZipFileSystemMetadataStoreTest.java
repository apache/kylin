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

import static org.apache.kylin.common.persistence.metadata.FileSystemFilterFactory.convertConditionsToFilter;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.resources.ModelRawResource;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.var;

/**
 * 27/12/2023 hellozepp(lisheng.zhanglin@163.com)
 */
public class ZipFileSystemMetadataStoreTest extends NLocalFileMetadataTestCase {
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
    public void testListAllZipMeta() {
        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().readonly(true).processor(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setMetadataUrl("src/test/resources/ut_meta/@file,zip=1");
            final Properties props = config.exportToProperties();
            KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
            val store = ResourceStore.getKylinMetaStore(dstConfig);
            FileSystemMetadataStore metadataStore = (FileSystemMetadataStore) store.getMetadataStore();
            NavigableSet<String> set = metadataStore.listAll();
            Assert.assertTrue(set.contains("MODEL/9852f045-040c-41aa-acf5-597d9507b66a"));
            return 0;
        }).unitName(UnitOfWork.GLOBAL_UNIT).maxRetry(3).build());
    }

    @Test
    public void testDumpZipMeta() {
        val metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        // Notice: The data directly is src/test/resources/ut_meta without copy, do not modify
        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().readonly(true).processor(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setMetadataUrl("src/test/resources/ut_meta/@file,zip=1");
            Properties props = config.exportToProperties();
            KylinConfig sourceConfig = KylinConfig.createKylinConfig(props);
            var sourceStore = ResourceStore.getKylinMetaStore(sourceConfig);
            FileSystemMetadataStore metadataStore = (FileSystemMetadataStore) sourceStore.getMetadataStore();
            List<RawResource> modelRaw = metadataStore.get(MetadataType.MODEL, new RawResourceFilter(), true, true);
            Assert.assertFalse(modelRaw.isEmpty());

            KylinConfig descConf = KylinConfig.getInstanceFromEnv();
            // Create file system and dump
            descConf.setMetadataUrl(metadataUrl.getIdentifier());
            KylinConfig dstConfig = KylinConfig.createKylinConfig(descConf.exportToProperties());
            metadataStore = (FileSystemMetadataStore) ResourceStore.getKylinMetaStore(dstConfig).getMetadataStore();
            metadataStore.dump(sourceStore,
                    modelRaw.stream().map(RawResource::generateKeyWithType).collect(Collectors.toList()));
            Map<String, RawResource> rawResourceFromUploadFile = getRawResourceFromUploadFile(
                    new File(metadataUrl.getIdentifier() + "/metadata.zip"));
            Assert.assertTrue(rawResourceFromUploadFile.containsKey("MODEL/9852f045-040c-41aa-acf5-597d9507b66a"));
            return 0;
        }).unitName(UnitOfWork.GLOBAL_UNIT).maxRetry(3).build());
    }

    @Test
    public void testGetZipMeta() {
        // Notice: The data directly is src/test/resources/ut_meta without copy, do not modify
        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().processor(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setMetadataUrl("src/test/resources/ut_meta/@file,zip=1");
            final Properties props = config.exportToProperties();
            KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
            val store = ResourceStore.getKylinMetaStore(dstConfig);
            RawResource resource = store.getResource("MODEL/9852f045-040c-41aa-acf5-597d9507b66a");
            Assert.assertEquals("model_agg_update", resource.getProject());
            Assert.assertEquals("ssb_model", ((ModelRawResource) resource).getAlias());

            RawResourceFilter rawResourceFilter = new RawResourceFilter();
            // Only Support outer
            rawResourceFilter.addConditions(new RawResourceFilter.Condition("alias", Collections.singletonList("ssb_"),
                    RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE));
            rawResourceFilter.addConditions(new RawResourceFilter.Condition("project",
                    Collections.singletonList("MODEL_AGG_UPDATE"), RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE));
            List<RawResource> model = store.getMatchedResourcesWithoutContent("MODEL", rawResourceFilter);
            Assert.assertEquals(1, model.size());

            FileSystemMetadataStore metadataStore = (FileSystemMetadataStore) store.getMetadataStore();

            // Using complex filter
            List<RawResource> rawResources = metadataStore.get(MetadataType.MODEL, rawResourceFilter, true, true);
            Assert.assertEquals(1, rawResources.size());

            // Using path filter
            rawResourceFilter = new RawResourceFilter();
            rawResourceFilter.addConditions(
                    new RawResourceFilter.Condition("metaKey", Collections.singletonList("MODEL_agg_update.SSB."),
                            RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE));

            var context = convertConditionsToFilter(rawResourceFilter, MetadataType.TABLE_INFO);
            String resPath = context.getResPath();
            var regex = context.getRegex();
            var jsonFilter = context.getRawResourceFilter();
            Assert.assertEquals("TABLE_INFO", resPath);
            Assert.assertEquals("(?i).*\\QMODEL_agg_update.SSB.\\E.*", regex);
            Assertions.assertThat(jsonFilter.getConditions().isEmpty()).isTrue();
            Assert.assertFalse(context.isWholePath());

            rawResources = metadataStore.get(MetadataType.TABLE_INFO, rawResourceFilter, true, true);
            Assert.assertEquals(2, rawResources.size());
            List<String> metakeys = new ArrayList<>();
            metakeys.add("model_agg_update.SSB.P_LINEORDER");
            metakeys.add("model_agg_update.SSB.CUSTOMER");
            rawResources.forEach(x -> Assert.assertTrue(metakeys.contains(x.getMetaKey())));

            // IN Operator
            RawResourceFilter filter1 = new RawResourceFilter();
            filter1.addConditions(new RawResourceFilter.Condition("metaKey",
                    Arrays.asList("model_agg_update.SSB.P_LINEORDER", "123"), RawResourceFilter.Operator.IN));
            context = convertConditionsToFilter(filter1, MetadataType.TABLE_INFO);
            Assert.assertEquals("\\Qmodel_agg_update.SSB.P_LINEORDER.json\\E|\\Q123.json\\E", context.getRegex());
            var tableInfo = store.getMatchedResourcesWithoutContent("TABLE_INFO", filter1);
            Assert.assertEquals(1, tableInfo.size());
            Assert.assertEquals("model_agg_update.SSB.P_LINEORDER", tableInfo.get(0).getMetaKey());

            // case insensitive
            RawResourceFilter filter2 = new RawResourceFilter();
            filter2.addConditions(new RawResourceFilter.Condition("metaKey",
                    Collections.singletonList("model_agg_update.ssb.p_lineorder"),
                    RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE));
            context = convertConditionsToFilter(filter2, MetadataType.TABLE_INFO);
            Assert.assertEquals("(?i)\\Qmodel_agg_update.ssb.p_lineorder.json\\E", context.getRegex());
            tableInfo = store.getMatchedResourcesWithoutContent("TABLE_INFO", filter2);
            Assert.assertEquals(1, tableInfo.size());
            Assert.assertEquals("model_agg_update.SSB.P_LINEORDER", tableInfo.get(0).getMetaKey());
            return 0;
        }).unitName(UnitOfWork.GLOBAL_UNIT).maxRetry(1).build());
    }

    public static Map<String, RawResource> getRawResourceFromUploadFile(File uploadFile) {
        val resourceMap = FileSystemMetadataStore.getFilesFromCompressedFile(new Path(uploadFile.toURI()),
                new FileSystemMetadataStore.CompressHandler(), HadoopUtil.getWorkingFileSystem());
        val filesFromCompressedFile = Maps.<String, RawResource> newHashMap();
        resourceMap.forEach((k, v) -> filesFromCompressedFile.put(k.replaceAll(".json", ""), v));
        return filesFromCompressedFile;
    }
}
