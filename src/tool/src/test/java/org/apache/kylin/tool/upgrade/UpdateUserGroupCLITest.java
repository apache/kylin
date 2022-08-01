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

import static org.apache.kylin.common.persistence.ResourceStore.USER_GROUP_ROOT;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UpdateUserGroupCLITest extends NLocalFileMetadataTestCase {
    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpgrade() throws Exception {
        String metadata = KylinConfig.getInstanceFromEnv().getMetadataUrl().toString();
        File path = new File(metadata);
        File userGroupFile = new File(path, USER_GROUP_ROOT);
        Assert.assertTrue(userGroupFile.isFile());
        UpdateUserGroupCLI tool = new UpdateUserGroupCLI();
        tool.execute(new String[] { "-metadata_dir", path.getAbsolutePath(), "-e" });
        Assert.assertTrue(userGroupFile.isDirectory());
        Assert.assertEquals(4, userGroupFile.listFiles().length);
        Set<String> groupNames = new HashSet<>();
        for (File file : userGroupFile.listFiles()) {
            UserGroup userGroup = JsonUtil.readValue(file, UserGroup.class);
            groupNames.add(userGroup.getGroupName());
            Assert.assertEquals(userGroup.getGroupName(), file.getName());
        }
        Assert.assertEquals(4, groupNames.size());
        Assert.assertTrue(groupNames.contains("ALL_USERS"));
        Assert.assertTrue(groupNames.contains("ROLE_ADMIN"));
        Assert.assertTrue(groupNames.contains("ROLE_ANALYST"));
        Assert.assertTrue(groupNames.contains("ROLE_MODELER"));
    }

}
