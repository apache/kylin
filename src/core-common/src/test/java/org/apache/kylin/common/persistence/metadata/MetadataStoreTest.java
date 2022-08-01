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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.MetadataChecker;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.val;

@MetadataInfo(onlyProps = true)
public class MetadataStoreTest {

    @Test
    public void testVerify(@TempDir File junitFolder) throws Exception {
        //copy an metadata image to junit folder
        ResourceTool.copy(getTestConfig(), KylinConfig.createInstanceFromUri(junitFolder.getAbsolutePath()),
                "/_global/project/default.json");
        ResourceTool.copy(getTestConfig(), KylinConfig.createInstanceFromUri(junitFolder.getAbsolutePath()),
                "/default");

        getTestConfig().setMetadataUrl(junitFolder.getAbsolutePath());
        val metadataStore = MetadataStore.createMetadataStore(getTestConfig());
        MetadataChecker metadataChecker = new MetadataChecker(metadataStore);

        //add illegal file,the verify result is not qualified
        Paths.get(junitFolder.getAbsolutePath(), "/IllegalFile").toFile().createNewFile();
        val verifyResultWithIllegalFile = metadataChecker.verify();
        Assertions.assertThat(verifyResultWithIllegalFile.getIllegalFiles()).hasSize(1).contains("/IllegalFile");
        assertFalse(verifyResultWithIllegalFile.isQualified());
        Paths.get(junitFolder.getAbsolutePath(), "/IllegalFile").toFile().delete();

        //add illegal project dir ,the verify result is not qualified
        Paths.get(junitFolder.getAbsolutePath(), "/IllegalProject").toFile().mkdir();
        Paths.get(junitFolder.getAbsolutePath(), "/IllegalProject/test.json").toFile().createNewFile();
        val verifyResultWithIllegalProject = metadataChecker.verify();
        Assertions.assertThat(verifyResultWithIllegalProject.getIllegalProjects()).hasSize(1)
                .contains("IllegalProject");
        Assertions.assertThat(verifyResultWithIllegalProject.getIllegalFiles()).hasSize(1)
                .contains("/IllegalProject/test.json");
        assertFalse(verifyResultWithIllegalProject.isQualified());
        Paths.get(junitFolder.getAbsolutePath(), "/IllegalProject/test.json").toFile().delete();
        Paths.get(junitFolder.getAbsolutePath(), "/IllegalProject").toFile().delete();

        //add legal project and file,the verify result is qualified
        Paths.get(junitFolder.getAbsolutePath(), "/legalProject").toFile().mkdir();
        Paths.get(junitFolder.getAbsolutePath(), "/_global").toFile().mkdir();
        Paths.get(junitFolder.getAbsolutePath(), "/_global/project").toFile().mkdir();
        Paths.get(junitFolder.getAbsolutePath(), "/_global/project/legalProject.json").toFile().createNewFile();
        val verifyResultWithLegalProject = metadataChecker.verify();
        Assertions.assertThat(verifyResultWithLegalProject.getIllegalFiles()).isEmpty();
        Assertions.assertThat(verifyResultWithLegalProject.getIllegalProjects()).isEmpty();
        assertTrue(verifyResultWithLegalProject.isQualified());

        //the metadata dir doesn't have uuid file
        assertFalse(metadataChecker.verify().isExistUUIDFile());
        Paths.get(junitFolder.getAbsolutePath(), "/UUID").toFile().createNewFile();
        assertTrue(metadataChecker.verify().isExistUUIDFile());

        //the metadata dir doesn't have user group file
        assertFalse(metadataChecker.verify().isExistUserGroupFile());
        Files.createFile(Paths.get(junitFolder.getAbsolutePath(), "/_global/user_group"));
        assertTrue(metadataChecker.verify().isExistUserGroupFile());

        //the metadata dir doesn't have user dir
        assertFalse(metadataChecker.verify().isExistUserDir());
        Paths.get(junitFolder.getAbsolutePath(), "/_global/user").toFile().mkdir();
        Files.createFile(Paths.get(junitFolder.getAbsolutePath(), "/_global/user/ADMIN"));
        assertTrue(metadataChecker.verify().isExistUserDir());

        //the metadata dir doesn't have acl dir
        assertFalse(metadataChecker.verify().isExistACLDir());
        Paths.get(junitFolder.getAbsolutePath(), "/_global/acl").toFile().mkdir();
        Files.createFile(Paths.get(junitFolder.getAbsolutePath(), "/_global/acl/test"));
        assertTrue(metadataChecker.verify().isExistACLDir());
    }
}
