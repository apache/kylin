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

import static org.apache.kylin.common.util.HadoopUtil.MAPR_FS_PREFIX;
import static org.apache.kylin.common.util.HadoopUtil.readStringFromHdfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.val;

@MetadataInfo(project = "ssb")
class HadoopUtilTest {

    @TempDir
    Path tempDir;

    @Test
    void testGetFileStatusPathsFromHDFSDir_Dir() throws IOException {
        File mainDir = new File(tempDir.toFile(), "tt");
        FileUtils.forceMkdir(mainDir);
        Assertions.assertTrue(mainDir.exists());

        val fileStatus = HadoopUtil.getFileStatusPathsFromHDFSDir(tempDir.toString(), false);

        Assertions.assertEquals(1, fileStatus.size());
        Assertions.assertTrue(fileStatus.get(0).isDirectory());
    }

    @Test
    void testGetFileStatusPathsFromHDFSDIR_File() throws IOException {
        File tmpFile = new File(tempDir.toFile(), "abc.log");

        Assertions.assertTrue(tmpFile.createNewFile());
        Assertions.assertTrue(tmpFile.exists());

        val fileStatus = HadoopUtil.getFileStatusPathsFromHDFSDir(tempDir.toString(), true);

        Assertions.assertEquals(1, fileStatus.size());
        Assertions.assertTrue(fileStatus.get(0).isFile());
    }

    @Test
    void testMkdirIfNotExist_NotExist() {
        File mainDir = new File(tempDir.toFile(), "tt");

        Assertions.assertFalse(mainDir.exists());

        HadoopUtil.mkdirIfNotExist(mainDir.getAbsolutePath());

        Assertions.assertTrue(mainDir.exists());
        Assertions.assertTrue(mainDir.isDirectory());
    }

    @Test
    void testMkdirIfNotExist_Existed() throws IOException {
        File mainDir = new File(tempDir.toFile(), "tt");
        FileUtils.forceMkdir(mainDir);
        Assertions.assertTrue(mainDir.exists());

        HadoopUtil.mkdirIfNotExist(mainDir.getAbsolutePath());

        Assertions.assertTrue(mainDir.exists());
        Assertions.assertTrue(mainDir.isDirectory());
    }

    @Test
    void testDeletePath() throws IOException {
        File mainDir = new File(tempDir.toFile(), "testDeletePath");
        {
            Assertions.assertFalse(mainDir.exists());
            val deleteSuccess = HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(),
                    new org.apache.hadoop.fs.Path(mainDir.getAbsolutePath()));
            Assertions.assertFalse(deleteSuccess);
        }

        {
            FileUtils.forceMkdir(mainDir);
            Assertions.assertTrue(mainDir.exists());
            val deleteSuccess = HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(),
                    new org.apache.hadoop.fs.Path(mainDir.getAbsolutePath()));
            Assertions.assertTrue(deleteSuccess);
        }
    }

    @Test
    void testGetPathWithoutScheme() {
        {
            val pathStr = "file://asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals(pathStr, path);
        }

        {
            val pathStr = "file:/asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals("file:///asdasd", path);
        }

        {
            val pathStr = MAPR_FS_PREFIX + "asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals("asdasd", path);
        }

        {
            val pathStr = "xxx://asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals("xxx://asdasd/", path);
        }
    }

    @Test
    void testToBytes() {
        val arrayWritable = new ArrayWritable(new String[] { "a" });
        val resultBytes = HadoopUtil.toBytes(arrayWritable);
        Assertions.assertNotNull(resultBytes);
    }

    @Test
    void testFixWindowsPath() {
        {
            val pathStr = "C:\\\\//asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///C:////asdasd", path);
        }

        {
            val pathStr = "D:\\\\//asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///D:////asdasd", path);
        }

        {
            val pathStr = "C:///asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///C:///asdasd", path);
        }

        {
            val pathStr = "D:///asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///D:///asdasd", path);
        }
    }

    @Test
    void testMakeURI() {
        {
            val pathStr = "C:\\\\//asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///C:////asdasd", path.toString());
        }

        {
            val pathStr = "D:\\\\//asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///D:////asdasd", path.toString());
        }

        {
            val pathStr = "C:///asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///C:///asdasd", path.toString());
        }

        {
            val pathStr = "D:///asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///D:///asdasd", path.toString());
        }
    }

    @Test
    void testWriteStringToHdfsAndRead() throws IOException {
        String strWrite = "STA-1:start,event=cpu";
        File profileFlagDir = new File(tempDir.toFile(), "profiler_flags");
        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(profileFlagDir.getAbsolutePath());
        HadoopUtil.writeStringToHdfs(strWrite, hdfsPath);
        Assertions.assertEquals(strWrite, readStringFromHdfs(hdfsPath));
    }

    @Test
    void testGetWritingClusterFileSystem() {
        Assertions.assertNotNull(HadoopUtil.getWritingClusterFileSystem());
    }
}
