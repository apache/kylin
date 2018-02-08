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
    public static String LOCALMETA_TEMP_DATA = "../examples/test_metadata/";

    @Override
    public void createTestMetadata(String... overlayMetadataDirs) {
        //overlayMetadataDirs is useless yet
        staticCreateTestMetadata(overlayMetadataDirs);
    }

    public static void staticCreateTestMetadata(String... overlayMetadataDirs) {
        String testDataFolder = LOCALMETA_TEST_DATA;
        KylinConfig.destroyInstance();

        String tempTestMetadataUrl = LOCALMETA_TEMP_DATA;
        try {
            FileUtils.deleteDirectory(new File(tempTestMetadataUrl));
            FileUtils.copyDirectory(new File(testDataFolder), new File(tempTestMetadataUrl));

            for (String overlay : overlayMetadataDirs) {
                FileUtils.copyDirectory(new File(overlay), new File(tempTestMetadataUrl));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, tempTestMetadataUrl);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl(tempTestMetadataUrl);
        config.setProperty("kylin.env.hdfs-working-dir", "file:///tmp/kylin");
    }

    public static void cleanAfterClass() {
        File directory = new File(LOCALMETA_TEMP_DATA);
        try {
            FileUtils.deleteDirectory(directory);
        } catch (IOException e) {
            if (directory.exists() && directory.list().length > 0)
                throw new IllegalStateException("Can't delete directory " + directory, e);
        }
        staticCleanupTestMetadata();
    }

    @Override
    public void cleanupTestMetadata() {
        cleanAfterClass();
    }

    protected String getLocalWorkingDirectory() {
        String dir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        if (dir.startsWith("file://"))
            dir = dir.substring("file://".length());
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected ResourceStore getStore() {
        return ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }
}
