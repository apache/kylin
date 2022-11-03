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

import org.junit.Assert;
import org.junit.Test;

public class FilePathUtilTest {

    @Test
    public void testReturnFilePathIfExists() throws IOException {
        // null
        Assert.assertTrue(FilePathUtil.returnFilePathIfExists(null).isEmpty());
        // singleFilePath null or empty
        Assert.assertTrue(FilePathUtil.returnFilePathIfExists(null, "").isEmpty());
        // singleFilePath null but not empty
        Assert.assertTrue(FilePathUtil.returnFilePathIfExists(null, "notEmpty").isEmpty());
        // singleFilePath not null but empty
        Assert.assertTrue(FilePathUtil.returnFilePathIfExists("notNull", "").isEmpty());
        // file not exists
        Assert.assertTrue("", FilePathUtil.returnFilePathIfExists("testPath").isEmpty());
        // directory exists but is not a file
        File tempFile = File.createTempFile("start-", "-end");
        Assert.assertTrue(FilePathUtil.returnFilePathIfExists(tempFile.getParent()).isEmpty());
        // The file exists and is a real file
        Assert.assertFalse(FilePathUtil.returnFilePathIfExists(tempFile.getAbsolutePath()).isEmpty());
        tempFile.deleteOnExit();
    }
}