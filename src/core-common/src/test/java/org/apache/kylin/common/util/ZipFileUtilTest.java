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
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.val;

public class ZipFileUtilTest {

    @Test
    public void testCompressAndDecompressZipFile(@TempDir File tempDir) throws IOException {
        String mainDir = tempDir.getAbsolutePath() + "/testCompressZipFile";

        File compressDir = new File(mainDir, "compress_dir");
        FileUtils.forceMkdir(compressDir);

        FileUtils.writeStringToFile(new File(compressDir, "a.txt"), "111111111111");
        FileUtils.writeStringToFile(new File(compressDir, "b.txt"), "222222222222");
        FileUtils.forceMkdir(new File(compressDir, "c"));
        FileUtils.writeStringToFile(new File(compressDir, "c/c1.txt"), "333333333333");

        String zipFilename = compressDir.getAbsolutePath() + ".zip";
        ZipFileUtils.compressZipFile(compressDir.getAbsolutePath(), zipFilename);

        Assert.assertTrue(new File(zipFilename).exists() && new File(zipFilename).length() > 200);

        File decompressDir = new File(mainDir, "decompress_dir");
        FileUtils.forceMkdir(decompressDir);
        ZipFileUtils.decompressZipFile(zipFilename, decompressDir.getAbsolutePath());

        val aFile = new File(decompressDir.getAbsolutePath(), "compress_dir/a.txt");
        val c1File = new File(decompressDir.getAbsolutePath(), "compress_dir/c/c1.txt");
        Assert.assertTrue(aFile.exists());
        Assert.assertEquals("111111111111", FileUtils.readFileToString(aFile));
        Assert.assertEquals("333333333333", FileUtils.readFileToString(c1File));
    }

    @Test
    public void testCompressEmptyDirZipFile(@TempDir File tempDir) throws IOException {
        String mainDir = tempDir.getAbsolutePath() + "/testCompressZipFile";

        File compressDir = new File(mainDir, "compress_dir");
        FileUtils.forceMkdir(compressDir);

        FileUtils.writeStringToFile(new File(compressDir, "a.txt"), "111111111111");
        FileUtils.writeStringToFile(new File(compressDir, "b.txt"), "222222222222");
        File emptyDirectory = new File(compressDir, "empty_directory");
        emptyDirectory.mkdir();
        String zipFilename = compressDir.getAbsolutePath() + ".zip";
        ZipFileUtils.compressZipFile(compressDir.getAbsolutePath(), zipFilename);

        long fileCount = new ZipFile(zipFilename).stream().count();
        Assert.assertEquals(3, fileCount);
    }
}
