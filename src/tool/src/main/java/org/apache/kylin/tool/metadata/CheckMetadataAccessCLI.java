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

package org.apache.kylin.tool.metadata;

import static org.apache.kylin.common.persistence.MetadataType.PROJECT;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckMetadataAccessCLI {
    protected static final Logger logger = LoggerFactory.getLogger(CheckMetadataAccessCLI.class);

    public static void main(String[] args) {
        StorageURL metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        if (metadataUrl.metadataLengthIllegal()) {
            logger.info("the maximum length of metadata_name allowed is {}", StorageURL.METADATA_MAX_LENGTH);
            Unsafe.systemExit(1);
        }
        CheckMetadataAccessCLI cli = new CheckMetadataAccessCLI();

        if (args.length != 1) {
            logger.info("Usage: CheckMetadataAccessCLI <repetition>");
            Unsafe.systemExit(1);
        }

        long repetition = Long.parseLong(args[0]);

        while (repetition > 0) {
            if (!cli.testAccessMetadata()) {
                logger.error("Test failed.");
                Unsafe.systemExit(1);
            }
            repetition--;
        }

        logger.info("Test succeed.");
        Unsafe.systemExit(0);
    }

    public boolean testAccessMetadata() {

        String projectName = RandomUtil.randomUUIDStr();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore store = ResourceStore.getKylinMetaStore(config);

        logger.info("Start to test. Test metastore is: " + config.getMetadataUrl().toString());
        //test store's connection.
        try {
            store.listResourcesRecursively(PROJECT.name());
        } catch (Exception e) {
            logger.error("Connection test failed." + e.getCause(), e);
            return false;
        }

        //Test CURD

        //test create.
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).createProject(projectName, "test",
                        "This is a test project", null);
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (TransactionException e) {
            logger.error("Creation test failed." + e.getMessage());
            return false;
        }

        NProjectManager projectManager = NProjectManager.getInstance(config);
        if (projectManager.getProject(projectName) == null) {
            logger.error("Creation test failed.");
            return false;
        }

        //test update.
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                        .getProject(projectName);
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject(projectInstance,
                        projectName, "Still a test project", null);
                return null;
            }, projectName);
        } catch (TransactionException e) {
            logger.error("Update test failed." + e.getMessage());
            clean(store, MetadataType.mergeKeyWithType(projectName, PROJECT));
            return false;
        }

        //test delete
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).forceDropProject(projectName);
                return null;
            }, projectName);
        } catch (TransactionException e) {
            clean(store, MetadataType.mergeKeyWithType(projectName, PROJECT));
            logger.error("Deletion test failed");
            return false;
        }

        return true;
    }

    private void clean(ResourceStore store, String path) {
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                store.deleteResource(path);
                return null;
            }, path.split("/")[0]);
        } catch (TransactionException e) {
            throw new RuntimeException("Failed to cleanup test metadata, it will remain in the resource store: "
                    + store.getConfig().getMetadataUrl(), e);
        }
    }
}
