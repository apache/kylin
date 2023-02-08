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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import lombok.val;

public class RenameUserResourceToolTest extends NLocalFileMetadataTestCase {

    private KylinConfig config;

    @Before
    public void setup() throws IOException {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
        config = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    /**
     * rename_user_1 -> rename_user_11
     * <p>
     * rename_user_1 is rename_project_1's project management
     * rename_user_1 is rename_project_2's project owner model owner
     */
    @Test
    public void testRenameUser() {
        String data = "y\r\n";
        InputStream stdin = System.in;
        try {
            System.setIn(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())));
            val renameResourceTool = new RenameUserResourceTool();
            renameResourceTool.execute(new String[] { "-dir", config.getMetadataUrl().toString(), "--user",
                    "rename_user_1", "--collect-only", "false" });

            config.clearManagers();
            ResourceStore.clearCache();

            val projectManager = NProjectManager.getInstance(config);

            // project owner
            val project2 = projectManager.getProject("rename_project_2");
            Assert.assertEquals("rename_user_11", project2.getOwner());

            // model owner
            val dataModelManager = NDataModelManager.getInstance(config, "rename_project_2");
            val dataModel = dataModelManager.getDataModelDescByAlias("rename_model_2");
            Assert.assertEquals("rename_user_11", dataModel.getOwner());

            config.clearManagers();
            ResourceStore.clearCache();
            ResourceStore resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

            // project acl
            String projectAclPath = String.format(Locale.ROOT, "/_global/acl/%s", project2.getUuid());
            RawResource rs = resourceStore.getResource(projectAclPath);
            boolean renamed = false;
            try (InputStream is = rs.getByteSource().openStream()) {
                JsonNode aclJsonNode = JsonUtil.readValue(is, JsonNode.class);

                if (aclJsonNode.has("entries")) {
                    ArrayNode entries = (ArrayNode) aclJsonNode.get("entries");
                    for (JsonNode entry : entries) {
                        // p for person
                        if (entry.has("p")) {
                            String p = entry.get("p").asText();
                            if (StringUtils.equals(p, "rename_user_11")) {
                                renamed = true;
                                Assert.assertEquals(16, entry.get("m").intValue());
                            }
                        }
                    }
                }

            } catch (IOException e) {
            }

            Assert.assertTrue(renamed);

            renamed = false;
            val project1 = projectManager.getProject("rename_project_1");
            projectAclPath = String.format(Locale.ROOT, "/_global/acl/%s", project1.getUuid());
            rs = resourceStore.getResource(projectAclPath);
            try (InputStream is = rs.getByteSource().openStream()) {
                JsonNode aclJsonNode = JsonUtil.readValue(is, JsonNode.class);

                if (aclJsonNode.has("entries")) {
                    ArrayNode entries = (ArrayNode) aclJsonNode.get("entries");
                    for (JsonNode entry : entries) {
                        // p for person
                        if (entry.has("p")) {
                            String p = entry.get("p").asText();
                            if (StringUtils.equals(p, "rename_user_11")) {
                                renamed = true;
                                Assert.assertEquals(32, entry.get("m").intValue());
                            }
                        }
                    }
                }

            } catch (IOException e) {
            }
            Assert.assertTrue(renamed);

            config.clearManagers();
            ResourceStore.clearCache();
            config.clearManagersByProject("rename_project_1");

            // acl
            AclTCRManager tcrManager = AclTCRManager.getInstance(config, "rename_project_1");
            Assert.assertNotNull(tcrManager.getAclTCR("rename_user_11", true));

            config.clearManagers();
            ResourceStore.clearCache();
            config.clearManagersByProject("rename_project_2");

            tcrManager = AclTCRManager.getInstance(config, "rename_project_2");
            Assert.assertNotNull(tcrManager.getAclTCR("rename_user_11", true));

        } finally {
            System.setIn(stdin);
        }
    }
}
