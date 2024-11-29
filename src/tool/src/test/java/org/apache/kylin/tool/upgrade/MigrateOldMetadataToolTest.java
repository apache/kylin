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
package org.apache.kylin.tool.upgrade;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 25/12/2023 hellozepp(lisheng.zhanglin@163.com)
 */
public class MigrateOldMetadataToolTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File("src/test/resources/migrate_metadata/result"));
    }

    @Test
    public void doMigrate() {
        expectNoException(() -> {
            String inputPath = "src/test/resources/migrate_metadata/normal/metadata";
            String outputPath = "src/test/resources/migrate_metadata/result";
            fixBrokenMetadata(inputPath);
            MigrateKEMetadataTool tool = new MigrateKEMetadataTool();
            tool.doMigrate(inputPath, outputPath);
            resetBrokenMetadata(inputPath, outputPath, tool);
            return true;
        });
    }

    @Test
    public void doMigrateWithoutConcurrency() {
        overwriteSystemProp("kylin.metadata.concurrency-process-metadata-size-enabled", "false");
        doMigrate();
    }

    private void fixBrokenMetadata(String inputPath) throws IOException {
        List<String> brokenPath = new ArrayList<>();
        brokenPath.add(inputPath + "/broken_test/dataflow/f1bb4bbd-a638-442b-a276-e301fde0d7f6.json");
        brokenPath.add(inputPath + "/broken_test/index_plan/039eef32-9691-4c88-93ba-d65c58a1ab7a.json");
        brokenPath.add(inputPath + "/broken_test/model_desc/3f8941de-d01c-42b8-91b5-44646390864b.json");

        for (String path : brokenPath) {
            File file = new File(path);
            String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            if (content.startsWith("{")) {
                throw new IllegalStateException("Json file : [" + path + "] is not broken!!");
            }
            FileUtils.writeStringToFile(file, "{" + content, StandardCharsets.UTF_8);
        }
    }

    private void resetBrokenMetadata(String inputPath, String outputPath, MigrateKEMetadataTool tool)
            throws IOException {
        List<String> brokenPath = new ArrayList<>();
        // new metadata
        String brokenUuid = MigrateKEMetadataTool.getUniqueUuid(tool.modelUuidMap, "broken_test",
                "f1bb4bbd-a638-442b-a276-e301fde0d7f6");
        brokenPath.add(outputPath + "/DATAFLOW/" + brokenUuid + ".json");
        brokenPath.add(outputPath + "/INDEX_PLAN/039eef32-9691-4c88-93ba-d65c58a1ab7a.json");
        brokenPath.add(outputPath + "/MODEL/3f8941de-d01c-42b8-91b5-44646390864b.json");
        // old metadata
        brokenPath.add(inputPath + "/broken_test/dataflow/f1bb4bbd-a638-442b-a276-e301fde0d7f6.json");
        brokenPath.add(inputPath + "/broken_test/index_plan/039eef32-9691-4c88-93ba-d65c58a1ab7a.json");
        brokenPath.add(inputPath + "/broken_test/model_desc/3f8941de-d01c-42b8-91b5-44646390864b.json");

        for (String path : brokenPath) {
            File file = new File(path);
            String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(file, content.substring(1), StandardCharsets.UTF_8);
        }
    }

    @Test
    public void doMigrateJson() {
        expectNoException(() -> {
            String inputPath = "src/test/resources/migrate_metadata/audit_log/ke_metadata_test.json";
            String outputPath = "src/test/resources/migrate_metadata/result/ke_metadata_test.json";
            MigrateKEMetadataTool.migrateMetaFromJsonList(inputPath, outputPath, false);
            return true;
        });
    }

    @Test
    public void doMigrateJsonList() {
        expectNoException(() -> {
            String inputFile = "src/test/resources/migrate_metadata/audit_log/ke_metadata_test_audit_log.json";
            String outputFile = "src/test/resources/migrate_metadata/result/ke_metadata_test_audit_log.json";
            MigrateKEMetadataTool.migrateMetaFromJsonList(inputFile, outputFile, true);
            return true;
        });
    }

    @Test
    public void doMigrateSchemaUtilsZip() {
        expectNoException(() -> {
            String inputPath = "src/test/resources/migrate_metadata/compress";
            String outputPath = "src/test/resources/migrate_metadata/result";
            MigrateKEMetadataTool.migrateUtMetaZipFiles(inputPath, outputPath);
            return true;
        });
    }

    @Test
    public void doBatchMigrateUtMeta() {
        expectNoException(() -> {
            List<String> inputPaths = new ArrayList<String>() {
                {
                    add("src/test/resources/migrate_metadata/batch_migrate/meta1/metadata");
                    add("src/test/resources/migrate_metadata/batch_migrate/meta2/metadata");
                }
            };
            List<String> outputPaths = new ArrayList<String>() {
                {
                    add("src/test/resources/migrate_metadata/result/batch_migrate/meta1/metadata");
                    add("src/test/resources/migrate_metadata/result/batch_migrate/meta2/metadata");
                }
            };
            for (int i = 0, inputPathsSize = inputPaths.size(); i < inputPathsSize; i++) {
                MigrateKEMetadataTool.main(new String[] { inputPaths.get(i), outputPaths.get(i) });
            }
            return true;
        });
    }

    @Test
    public void doMigrateExportedModelMeta() {
        expectNoException(() -> {
            String inputPath = "src/test/resources/migrate_metadata/model_meta_with_rec/metadata";
            String outputPath = "src/test/resources/migrate_metadata/result/metadata";
            MigrateKEMetadataTool tool = new MigrateKEMetadataTool();
            tool.doMigrate(inputPath, outputPath);
            return true;
        });
        
    }
    
    private void expectNoException(Callable<Boolean> callable) {
        try {
            callable.call();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMigrateUserAndGroups() throws Exception {
        String inputPath = "src/test/resources/migrate_metadata/user_and_groups/metadata";
        String outputPath = "src/test/resources/migrate_metadata/result/metadata";
        MigrateKEMetadataTool tool = new MigrateKEMetadataTool();
        tool.doMigrate(inputPath, outputPath);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl(outputPath);
        FileSystemMetadataStore store = new FileSystemMetadataStore(config);
        RawResourceFilter emptyFilter = new RawResourceFilter();
        Assert.assertEquals(1, store.get(MetadataType.USER_INFO, emptyFilter, false, false).size());
        Assert.assertEquals(4, store.get(MetadataType.USER_GROUP, emptyFilter, false, false).size());
        Assert.assertEquals(1, store.get(MetadataType.USER_GLOBAL_ACL, emptyFilter, false, false).size());
    }
}
