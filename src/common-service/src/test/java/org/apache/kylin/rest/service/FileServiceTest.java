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

package org.apache.kylin.rest.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kylin.common.constant.Constants.BACKSLASH;
import static org.apache.kylin.common.constant.Constants.METADATA_FILE;
import static org.apache.kylin.common.constant.Constants.SYSTEM_TMP_DIR;
import static org.apache.kylin.rest.service.FileService.METADATA_TMP_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Objects;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import lombok.val;

@MetadataInfo
class FileServiceTest {
    @Mock
    private FileService fileService = Mockito.spy(FileService.class);

    @Test
    void getMetadataBackupFromTmpPath() throws IOException {
        InputStream is = null;
        val tmpDirectory = Files.createTempDirectory(Paths.get(SYSTEM_TMP_DIR), METADATA_TMP_PREFIX).toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        val tmpFilePath = tmpFile.getAbsolutePath();
        try {
            is = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 0L);
            fail();
        } catch (IOException e) {
            assertInstanceOf(FileNotFoundException.class, e);
            assertEquals("Metadata backup temp file is not a file: " + tmpFilePath, e.getMessage());
        } finally {
            close(is);
        }

        try (val os = new FileOutputStream(tmpFile)) {
            os.write("123".getBytes(UTF_8));

            is = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 3L);
        } finally {
            close(is);
        }

        try {
            is = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 0L);
            fail();
        } catch (IOException e) {
            assertInstanceOf(FileNotFoundException.class, e);
            assertEquals("Metadata backup temp file length does not right: " + tmpFilePath + ", length :" + 3,
                    e.getMessage());
        } finally {
            close(is);
        }
    }

    private void close(InputStream is) {
        if (Objects.nonNull(is)) {
            try {
                is.close();
            } catch (IOException ignore) {
            }
        }
    }

    @Test
    void saveMetadataBackupInTmpPath() throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val path = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()),
                new Path(RandomUtil.randomUUIDStr(), METADATA_FILE));
        Pair<String, Long> result = null;
        try {
            result = fileService.saveMetadataBackupInTmpPath(path.toString());
            fail();
        } catch (IOException e) {
            assertInstanceOf(FileNotFoundException.class, e);
            assertEquals("Metadata backup file is not a file: " + path, e.getMessage());
            assertNull(result);
        }

        try (val out = fileSystem.create(path, true)) {
            out.write("123".getBytes(UTF_8));
        }
        result = fileService.saveMetadataBackupInTmpPath(path.toString());
        assertEquals(3, result.getSecond());
        val tmpPath = result.getFirst();
        val tmpFile = new File(tmpPath);
        assertEquals(3, tmpFile.length());
    }

    @Test
    void saveMetadataBackupInTmpPathWithoutMetadataZipFile() throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val path = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()), RandomUtil.randomUUIDStr());
        try (val out = fileSystem.create(path, true)) {
            out.write("123".getBytes(UTF_8));
        }
        try {
            fileService.saveMetadataBackupInTmpPath(path.toString());
            fail();
        } catch (IOException e) {
            assertInstanceOf(FileNotFoundException.class, e);
            assertTrue(e.getMessage().endsWith("HDFS backup file:" + path + ", len: " + 3));
        }
    }

    @Test
    void saveMetadataBackupTmpFromRequest() throws IOException {
        InputStream is = null;
        val tmpDirectory = Files.createTempDirectory(Paths.get(SYSTEM_TMP_DIR), METADATA_TMP_PREFIX).toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        val tmpFilePath = tmpFile.getAbsolutePath();

        try (val os = new FileOutputStream(tmpFile)) {
            os.write("123".getBytes(UTF_8));
            is = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 3L);
        }
        try {
            fileService.saveMetadataBackupTmpFromRequest(0L, is);
            fail();
        } catch (IOException e) {
            assertInstanceOf(FileNotFoundException.class, e);
            assertTrue(e.getMessage().startsWith("Metadata backup temp file length does not right: "));
            assertTrue(e.getMessage().endsWith(" length :" + 3));
        } finally {
            close(is);
        }

        try {
            is = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 3L);

            val tempPath = fileService.saveMetadataBackupTmpFromRequest(3L, is);
            val tempFile = new File(tempPath);
            assertEquals(3, tempFile.length());
        } finally {
            close(is);
        }
    }

    @Test
    void saveMetadataBackupInHDFS() throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val path = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()),
                new Path(RandomUtil.randomUUIDStr(), METADATA_FILE));
        val tmpDirectory = Files.createTempDirectory(METADATA_TMP_PREFIX).toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        val tmpFilePath = tmpFile.getAbsolutePath();
        try (val os = new FileOutputStream(tmpFile)) {
            os.write("123".getBytes(UTF_8));
        }

        try {
            fileService.saveMetadataBackupInHDFS(path.toString(), tmpFilePath, 0L);
            fail();
        } catch (IOException e) {
            assertInstanceOf(FileNotFoundException.class, e);
            assertEquals("Metadata backup temp file length does not right.\n Tmp file: " + tmpFilePath + " length: " + 0
                    + "\n DFS file: " + path + " length: " + 3, e.getMessage());
        }

        fileService.saveMetadataBackupInHDFS(path.toString(), tmpFilePath, 3L);
        val fileStatus = fileSystem.getFileStatus(path);
        assertEquals(3, fileStatus.getLen());
    }

    @Test
    void deleteTmpDir() throws IOException {
        val tmpDirectory = Files.createTempDirectory(METADATA_TMP_PREFIX).toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        val tmpFilePath = tmpFile.getAbsolutePath();
        try (val os = new FileOutputStream(tmpFile)) {
            os.write("123".getBytes(UTF_8));
        }
        fileService.deleteTmpDir(tmpFilePath);
        assertFalse(tmpDirectory.isDirectory());
    }

    @Test
    void downloadMetadataBackTmpFile() throws IOException {
        val tmpDirectory = Files.createTempDirectory(METADATA_TMP_PREFIX).toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        val tmpFilePath = tmpFile.getAbsolutePath();
        try (val os = new FileOutputStream(tmpFile)) {
            os.write("123".getBytes(UTF_8));
        }
        val restTemplate = Mockito.spy(RestTemplate.class);
        ReflectionTestUtils.setField(fileService, "restTemplate", restTemplate);
        Mockito.doReturn("123").when(restTemplate).execute(anyString(), any(HttpMethod.class),
                ArgumentMatchers.<RequestCallback> any(), ArgumentMatchers.<ResponseExtractor<String>> any());
        val result = fileService.downloadMetadataBackTmpFile(tmpFilePath, 3L, RandomUtil.randomUUIDStr(),
                AddressUtil.getLocalInstance());
        assertEquals("123", result);
    }

    @Test
    void saveBroadcastMetadataBackup() throws IOException {
        val tmpDirectory = Files.createTempDirectory(METADATA_TMP_PREFIX).toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        val tmpFilePath = tmpFile.getAbsolutePath();
        try (val os = new FileOutputStream(tmpFile)) {
            os.write("123".getBytes(UTF_8));
        }
        val restTemplate = Mockito.spy(RestTemplate.class);
        ReflectionTestUtils.setField(fileService, "restTemplate", restTemplate);
        Mockito.doReturn(tmpFilePath).when(restTemplate).execute(anyString(), any(HttpMethod.class),
                ArgumentMatchers.<RequestCallback> any(), ArgumentMatchers.<ResponseExtractor<String>> any());

        val backupDIr = RandomUtil.randomUUIDStr();
        fileService.saveBroadcastMetadataBackup(backupDIr, tmpFilePath, 3L, RandomUtil.randomUUIDStr(),
                AddressUtil.getLocalInstance());
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val path = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()),
                new Path(backupDIr, METADATA_FILE));
        assertTrue(fileSystem.isFile(path));
        val fileStatus = fileSystem.getFileStatus(path);
        assertEquals(3, fileStatus.getLen());
    }

    @Test
    void saveAndGet() throws IOException {
        InputStream is = null;
        val tmpDirectory = Files.createTempDirectory(Paths.get(SYSTEM_TMP_DIR), METADATA_TMP_PREFIX).toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        val tmpFilePath = tmpFile.getAbsolutePath();

        try (val os = new FileOutputStream(tmpFile)) {
            os.write("123".getBytes(UTF_8));
            is = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 3L);
        }

        try {
            is = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 3L);

            val tempPath = fileService.saveMetadataBackupTmpFromRequest(3L, is);
            val tempFile = new File(tempPath);
            assertEquals(3, tempFile.length());

            try (val iss = fileService.getMetadataBackupFromTmpPath(tmpFilePath, 3L)) {
                val tempPath2 = fileService.saveMetadataBackupTmpFromRequest(3L, iss);
                val tempFile2 = new File(tempPath2);
                assertEquals(3, tempFile2.length());
            }
        } finally {
            close(is);
        }
    }

    @Test
    void testGetSafeAbsolutePath() {
        val tmpPath = Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX);
        val tmpDir = tmpPath.toAbsolutePath().normalize().toString();
        val validFilePath = Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "validFile.txt");
        {
            String filePath = tmpPath.toString();
            String result = FileService.getSafeAbsolutePath(filePath);

            assertEquals(Paths.get(filePath).toAbsolutePath().normalize().toString(), result);
            assertTrue(result.startsWith(tmpDir));
        }
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "subdir", "file.txt").toString();
            String result = FileService.getSafeAbsolutePath(filePath);

            assertEquals(Paths.get(filePath).toAbsolutePath().normalize().toString(), result);
            assertTrue(result.startsWith(tmpDir));
        }
        {
            String result = FileService.getSafeAbsolutePath(SYSTEM_TMP_DIR + BACKSLASH + METADATA_TMP_PREFIX);

            assertEquals(tmpDir, result);
        }
        {
            String filePath = SYSTEM_TMP_DIR + BACKSLASH + METADATA_TMP_PREFIX + "/./validFile.txt";
            String result = FileService.getSafeAbsolutePath(filePath);

            assertEquals(validFilePath.toAbsolutePath().normalize().toString(), result);
            assertTrue(result.startsWith(tmpDir));
        }
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "subdir1", "..", "subdir2", "file.txt")
                    .toString();
            String result = FileService.getSafeAbsolutePath(filePath);

            assertEquals(Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "subdir2", "file.txt").toAbsolutePath()
                    .normalize().toString(), result);
            assertTrue(result.startsWith(tmpDir));
        }
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "level1", "level2", "level3", "file.txt")
                    .toString();
            String result = FileService.getSafeAbsolutePath(filePath);

            assertEquals(Paths.get(filePath).toAbsolutePath().normalize().toString(), result);
            assertTrue(result.startsWith(tmpDir));
        }
        {
            String filePath = Paths
                    .get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "..", tmpPath.getFileName().toString(), "validFile.txt")
                    .toString();
            String result = FileService.getSafeAbsolutePath(filePath);

            assertEquals(validFilePath.toAbsolutePath().normalize().toString(), result);
            assertTrue(result.startsWith(tmpDir));
        }
        {
            String filePath = Paths
                    .get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "..", "..", tmpPath.toString(), "validFile.txt")
                    .toString();
            String result = FileService.getSafeAbsolutePath(filePath);

            assertEquals(validFilePath.toAbsolutePath().normalize().toString(), result);
            assertTrue(result.startsWith(tmpDir));
        }
    }

    @Test
    void testGetSafeAbsolutePathErrorWithDot() {
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, "..", "..", "etc", "passwd").toString();
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
            assertTrue(exception.getMessage().contains(filePath));
        }
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "..", "..", "..", "etc", "passwd")
                    .toString();
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
            assertTrue(exception.getMessage().contains(filePath));
        }
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, "..", "malicious.txt").toString();
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
        }
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, METADATA_TMP_PREFIX, "..", "malicious.txt").toString();
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path not kylin metadata tmp directory"));
        }
        {
            String filePath = String.format(Locale.ROOT, "%s/%s/subdir/../../../../../../etc/passwd", SYSTEM_TMP_DIR,
                    METADATA_TMP_PREFIX);
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
        }
        {
            String filePath = "../../../etc/passwd";
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
        }
    }

    @Test
    void testGetSafeAbsolutePathError() {
        {
            String filePath = "/etc/passwd";
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
            assertTrue(exception.getMessage().contains(filePath));
        }
        {
            String filePath = "";
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
        }
        {
            // Test with null path
            assertThrows(Exception.class, () -> {
                FileService.getSafeAbsolutePath(null);
            });
        }
        {
            // Test path with only dots (current directory reference)
            String filePath = ".";
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
        }
        {
            // Test path with only double dots (parent directory reference)
            String filePath = "..";
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path outside base directory"));
        }
        {
            String filePath = Paths.get(SYSTEM_TMP_DIR, "malicious.txt").toString();
            SecurityException exception = assertThrows(SecurityException.class,
                    () -> FileService.getSafeAbsolutePath(filePath));

            assertTrue(exception.getMessage().contains("Path not kylin metadata tmp directory"));
        }
    }
}