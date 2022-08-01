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

package org.apache.kylin.rest.util;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampedRollingFileOutputDirTest {

    private static final Logger logger = LoggerFactory.getLogger("kybot");

    private static final String FILE_NAME_PREFIX = "dummy.";

    private File outputDir;

    @Before
    public void setup() throws Exception {
        outputDir = Files.createTempDirectory("TimestampedRollingFileOutputDirTest").toFile();
        logger.debug("created tmp dir {}", outputDir);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(outputDir);
    }

    private TimestampedRollingFileOutputDir newRollingOutputDir(int maxFileCount) {
        return new TimestampedRollingFileOutputDir(outputDir, FILE_NAME_PREFIX, maxFileCount);
    }

    private void validateCreatedFile(File file) {
        Assert.assertTrue(file.getPath().startsWith(outputDir.getPath()));
        Assert.assertTrue(file.getName().startsWith(FILE_NAME_PREFIX));

        long fileTS = Long.parseLong(file.getName().substring(FILE_NAME_PREFIX.length()));
        Assert.assertTrue(fileTS > new Date(0).getTime());
        Assert.assertTrue(fileTS <= System.currentTimeMillis());
    }

    private File newFile(TimestampedRollingFileOutputDir rollingFileOutputDir) throws IOException {
        long start = System.currentTimeMillis();
        await().until(() -> start != System.currentTimeMillis());
        return rollingFileOutputDir.newOutputFile();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCreationParamsEmptyName() {
        new TimestampedRollingFileOutputDir(outputDir, "", 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCreationParamsInvalidMaxFileCnt() {
        new TimestampedRollingFileOutputDir(outputDir, FILE_NAME_PREFIX, 0);
    }

    @Test
    public void testNewFileCreation() throws IOException {
        TimestampedRollingFileOutputDir rollingFileOutputDir = newRollingOutputDir(1);
        File file = newFile(rollingFileOutputDir);

        Assert.assertEquals(1, Objects.requireNonNull(outputDir.listFiles()).length);
        validateCreatedFile(file);
    }

    @Test
    public void testFileRollingCount1() throws IOException {
        TimestampedRollingFileOutputDir rollingFileOutputDir = newRollingOutputDir(1);
        File file1 = newFile(rollingFileOutputDir);
        Assert.assertEquals(1, Objects.requireNonNull(outputDir.listFiles()).length);
        File file2 = newFile(rollingFileOutputDir);
        Assert.assertEquals(1, Objects.requireNonNull(outputDir.listFiles()).length);
        Assert.assertNotEquals(file1.getPath(), file2.getPath());
        Assert.assertFalse(file1.exists());
        Assert.assertTrue(file2.exists());
    }

    @Test
    public void testFileRollingCount2() throws IOException {
        TimestampedRollingFileOutputDir rollingFileOutputDir = newRollingOutputDir(2);
        File file1 = newFile(rollingFileOutputDir);
        File file2 = newFile(rollingFileOutputDir);
        Assert.assertEquals(2, Objects.requireNonNull(outputDir.listFiles()).length);

        File file3 = newFile(rollingFileOutputDir);
        Assert.assertEquals(2, Objects.requireNonNull(outputDir.listFiles()).length);

        Assert.assertFalse(file1.exists());
        Assert.assertTrue(file2.exists());
        Assert.assertTrue(file3.exists());
    }

}
