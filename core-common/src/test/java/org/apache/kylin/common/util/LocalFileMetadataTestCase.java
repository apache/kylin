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

    public static final String LOCALMETA_TEST_DATA = "../examples/test_case_data/localmeta";
    public static final String LOCALMETA_TEMP_DATA = "../examples/test_metadata/";

    @Override
    public void createTestMetadata(String... overlayMetadataDirs) {
        staticCreateTestMetadata(true, new OverlayMetaHook(overlayMetadataDirs));
    }

    public static void staticCreateTestMetadata(String... overlayMetadataDirs) {
        staticCreateTestMetadata(true, new OverlayMetaHook(overlayMetadataDirs));
    }

    public static void staticCreateTestMetadata(boolean useTestMeta, MetadataTestCaseHook hook) {
        try {
            KylinConfig.destroyInstance();

            FileUtils.deleteDirectory(new File(LOCALMETA_TEMP_DATA));
            if (useTestMeta) {
                FileUtils.copyDirectory(new File(LOCALMETA_TEST_DATA), new File(LOCALMETA_TEMP_DATA));
            }

            if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null) {
                System.setProperty(KylinConfig.KYLIN_CONF, LOCALMETA_TEMP_DATA);
            }

            if (hook != null) {
                hook.hook();
            }
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setMetadataUrl(LOCALMETA_TEMP_DATA);
            config.setProperty("kylin.env.hdfs-working-dir", "file:///tmp/kylin");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    public interface MetadataTestCaseHook {
        void hook() throws IOException;
    }

    public static class OverlayMetaHook implements MetadataTestCaseHook {
        private String[] overlayMetadataDirs;

        public OverlayMetaHook(String... overlayMetadataDirs) {
            this.overlayMetadataDirs = overlayMetadataDirs;
        }

        @Override
        public void hook() throws IOException {
            //some test cases may require additional metadata entries besides standard test metadata in test_case_data/localmeta
            for (String overlay : overlayMetadataDirs) {
                FileUtils.copyDirectory(new File(overlay), new File(LOCALMETA_TEMP_DATA));
            }
        }
    }

    public static class ExcludeMetaHook implements MetadataTestCaseHook {
        private String[] excludeMetadataDirs;

        public ExcludeMetaHook(String... excludeMetadataDirs) {
            this.excludeMetadataDirs = excludeMetadataDirs;
        }

        @Override
        public void hook() throws IOException {
            //some test cases may want exclude metadata entries besides standard test metadata in test_case_data/localmeta
            for (String exclude : excludeMetadataDirs) {
                FileUtils.deleteQuietly(new File(exclude));
            }
        }
    }
}
