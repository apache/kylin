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
package org.apache.kylin.common.util;

import static org.apache.kylin.common.persistence.MetadataType.ALL_TYPE_STR;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.Getter;
import lombok.val;

public class MetadataChecker {

    private final MetadataStore metadataStore;

    private static final String LINE_BREAK = "\n";

    public MetadataChecker(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Getter
    public static class VerifyResult {
        boolean existUUIDFile = false;
        boolean existImageFile = false;
        boolean existACLDir = false;
        boolean existUserDir = false;
        boolean existUserGroupFile = false;
        boolean existCompressedFile = false;
        boolean existModelDescFile = false;
        boolean existIndexPlanFile = false;
        boolean existTable = false;
        boolean existVersionFile = false;
        private final Set<String> illegalTables = Sets.newHashSet();
        private final Set<String> illegalFiles = Sets.newHashSet();

        public boolean isQualified() {
            return illegalTables.isEmpty() && illegalFiles.isEmpty();
        }

        public boolean isModelMetadataQualified() {
            return illegalFiles.isEmpty() && existUUIDFile && existModelDescFile && existIndexPlanFile && existTable;
        }

        public String getResultMessage() {
            StringBuilder resultMessage = new StringBuilder();

            resultMessage.append("the uuid file exists : ").append(existUUIDFile).append(LINE_BREAK);
            resultMessage.append("the image file exists : ").append(existImageFile).append(LINE_BREAK);
            resultMessage.append("the user_group file exists : ").append(existUserGroupFile).append(LINE_BREAK);
            resultMessage.append("the user dir exist : ").append(existUserDir).append(LINE_BREAK);
            resultMessage.append("the acl dir exist : ").append(existACLDir).append(LINE_BREAK);

            if (!illegalTables.isEmpty()) {
                resultMessage.append("illegal projects : ").append(LINE_BREAK);
                for (String illegalTable : illegalTables) {
                    resultMessage.append("\t").append(illegalTable).append(LINE_BREAK);
                }
            }

            if (!illegalFiles.isEmpty()) {
                resultMessage.append("illegal files : ").append(LINE_BREAK);
                for (String illegalFile : illegalFiles) {
                    resultMessage.append("\t").append(illegalFile).append(LINE_BREAK);
                }
            }

            return resultMessage.toString();
        }

    }

    public static boolean verifyNonMetadataFile(String resourcePath) {
        return resourcePath.endsWith(".DS_Store") || resourcePath.endsWith("kylin.properties")
                || resourcePath.endsWith("_image");
    }

    public VerifyResult verify() {
        VerifyResult verifyResult = new VerifyResult();

        // The valid metadata image contains at least the following conditions：
        //     1.may have one UUID file
        //     2.may have one _global dir which may have one user_group file or one user dir or one acl dir
        //     3.all other subdir as a project and must have only one project.json file

        val allFiles = metadataStore.listAll();
        for (final String file : allFiles) {
            if (verifyNonMetadataFile(file)) {
                continue;
            }
            //check uuid file
            if (file.equals(ResourceStore.METASTORE_UUID_TAG)) {
                verifyResult.existUUIDFile = true;
                continue;
            }

            //check VERSION file
            if (file.equals(ResourceStore.VERSION_FILE)) {
                verifyResult.existVersionFile = true;
                continue;
            }

            //check user_group file
            if (file.startsWith(MetadataType.USER_GROUP.name())) {
                verifyResult.existUserGroupFile = true;
                continue;
            }

            //check user dir
            if (file.startsWith(MetadataType.USER_INFO.name())) {
                verifyResult.existUserDir = true;
                continue;
            }

            //check acl dir
            if (file.startsWith(MetadataType.OBJECT_ACL.name())) {
                verifyResult.existACLDir = true;
                continue;
            }

            if (file.equals(ResourceStore.METASTORE_IMAGE)) {
                verifyResult.existImageFile = true;
                continue;
            }

            if (file.startsWith(ResourceStore.COMPRESSED_FILE)) {
                verifyResult.existCompressedFile = true;
                continue;
            }

            //check illegal file which locates in metadata dir
            if (File.separator.equals(Paths.get(file).toFile().getParent())) {
                verifyResult.illegalFiles.add(file);
                continue;
            }

            //check metadata table dir
            final String tableName = Paths.get(file).getName(0).toString();
            if (!ALL_TYPE_STR.contains(tableName)) {
                verifyResult.illegalTables.add(tableName);
                verifyResult.illegalFiles.add(file);
            }
        }

        return verifyResult;

    }

    public VerifyResult verifyModelMetadata(List<String> resourcePath) {
        VerifyResult verifyResult = new VerifyResult();

        for (final String resoucePath : resourcePath) {
            // check uuid file
            if (resoucePath.equals(ResourceStore.METASTORE_UUID_TAG)) {
                verifyResult.existUUIDFile = true;
                continue;
            }

            //check VERSION file
            if (resoucePath.equals(ResourceStore.VERSION_FILE)) {
                verifyResult.existVersionFile = true;
                continue;
            }

            // check model desc
            if (resoucePath.startsWith(MetadataType.MODEL.name())) {
                verifyResult.existModelDescFile = true;
                continue;
            }

            // check index plan
            if (resoucePath.contains(MetadataType.INDEX_PLAN.name())) {
                verifyResult.existIndexPlanFile = true;
                continue;
            }

            // check table
            if (resoucePath.contains(MetadataType.TABLE_INFO.name())) {
                verifyResult.existTable = true;
                continue;
            }

            //check illegal file which locates in metadata dir
            if (!resoucePath.contains("/")) {
                verifyResult.illegalFiles.add(resoucePath);
            }
        }

        return verifyResult;
    }
}
