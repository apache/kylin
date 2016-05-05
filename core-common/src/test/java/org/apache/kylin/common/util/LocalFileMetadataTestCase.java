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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;

public class LocalFileMetadataTestCase extends AbstractKylinTestCase {

    public static String LOCALMETA_TEST_DATA = "../examples/test_case_data/localmeta";

    @Override
    public void createTestMetadata() {
        staticCreateTestMetadata(getLocalMetaTestData());
    }

    protected String getLocalMetaTestData() {
        return LOCALMETA_TEST_DATA;
    }

    public static void staticCreateTestMetadata() {
        staticCreateTestMetadata(LOCALMETA_TEST_DATA);
    }

    public static void staticCreateTestMetadata(String testDataFolder) {
        KylinConfig.destroyInstance();

        String tempTestMetadataUrl = "../examples/test_metadata";
        try {
            FileUtils.deleteDirectory(new File(tempTestMetadataUrl));
            FileUtils.copyDirectory(new File(testDataFolder), new File(tempTestMetadataUrl));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, tempTestMetadataUrl);

        KylinConfig.getInstanceFromEnv().setMetadataUrl(tempTestMetadataUrl);
    }

    public static void cleanAfterClass() {
        String tempTestMetadataUrl = "../examples/test_metadata";
        try {
            FileUtils.deleteDirectory(new File(tempTestMetadataUrl));
        } catch (IOException e) {
            throw new IllegalStateException("Can't delete directory " + tempTestMetadataUrl, e);
        }
        staticCleanupTestMetadata();
    }

    @Override
    public void cleanupTestMetadata() {
        cleanAfterClass();
    }

    protected ResourceStore getStore() {
        return ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }
}
