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

package org.apache.kylin.rest.job;

import static org.apache.kylin.common.util.LocalFileMetadataTestCase.LOCALMETA_TEMP_DATA;
import static org.apache.kylin.common.util.LocalFileMetadataTestCase.cleanAfterClass;
import static org.apache.kylin.common.util.LocalFileMetadataTestCase.staticCreateTestMetadata;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class MetadataCleanupJobTest {

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testCleanUp() throws Exception {
        // file resource store may lose timestamp precision with millis second, set last modified as 2000
        staticCreateTestMetadata(false, new ResetTimeHook(2000, "src/test/resources/test_meta"));
        MetadataCleanupJob metadataCleanupJob = new MetadataCleanupJob();
        Map<String, Long> cleanupMap = metadataCleanupJob.cleanup(false, 30);
        Assert.assertEquals(4, cleanupMap.size());
        for (long timestamp : cleanupMap.values()) {
            Assert.assertEquals(2000, timestamp);
        }
    }

    @Test
    public void testNotCleanUp() throws Exception {
        staticCreateTestMetadata(false, new ResetTimeHook(System.currentTimeMillis(), "src/test/resources/test_meta"));
        MetadataCleanupJob metadataCleanupJob = new MetadataCleanupJob();
        Map<String, Long> cleanupMap = metadataCleanupJob.cleanup(false, 30);
        Assert.assertEquals(0, cleanupMap.size());
    }

    private class ResetTimeHook extends LocalFileMetadataTestCase.OverlayMetaHook {
        private long lastModified;

        ResetTimeHook(long lastModified, String... overlayMetadataDirs) {
            super(overlayMetadataDirs);
            this.lastModified = lastModified;
        }

        @Override
        public void hook() throws IOException {
            super.hook();
            Collection<File> files = FileUtils.listFiles(new File(LOCALMETA_TEMP_DATA), null, true);
            for (File file : files) {
                file.setLastModified(lastModified);
            }
        }
    }
}
