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

import static org.apache.kylin.common.constant.Constants.BACKSLASH;
import static org.apache.kylin.common.constant.Constants.METADATA_FILE;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class FileService extends BasicService {
    @Autowired
    @Qualifier("normalRestTemplate")
    private RestTemplate restTemplate;

    public InputStream getMetadataBackupFromTmpPath(String tmpFilePath, Long fileSize) throws IOException {
        val metadataBackupTmp = new File(tmpFilePath);
        if (metadataBackupTmp.isFile()) {
            if (metadataBackupTmp.length() != fileSize) {
                throw new FileNotFoundException("Metadata backup temp file length does not right: " + tmpFilePath
                        + ", length :" + metadataBackupTmp.length());
            }
            return Files.newInputStream(metadataBackupTmp.toPath());
        }
        throw new FileNotFoundException("Metadata backup temp file is not a file: " + tmpFilePath);
    }

    /**
     * path must is metadata zip file path, if not will throw FileNotFoundException
     */
    public Pair<String, Long> saveMetadataBackupInTmpPath(String path) throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val filePath = new Path(path);
        if (fileSystem.isFile(filePath)) {
            val fileStatus = fileSystem.getFileStatus(filePath);
            val tempDirectory = Files.createTempDirectory("MetadataBackupTmp-").toFile();
            fileSystem.copyToLocalFile(false, filePath, new Path(tempDirectory.getAbsolutePath()), true);
            val tmpFile = new File(tempDirectory, METADATA_FILE);
            if (fileStatus.getLen() != tmpFile.length()) {
                throw new FileNotFoundException("Metadata backup temp file length does not right.\n File: "
                        + tmpFile.getAbsolutePath() + ", length :" + tmpFile.length() + ";\n HDFS backup file:" + path
                        + ", len: " + fileStatus.getLen());
            }
            log.info("Metadata backup temp file path is [{}]", tmpFile.getAbsolutePath());
            return Pair.newPair(tmpFile.getAbsolutePath(), tmpFile.length());
        }
        throw new FileNotFoundException("Metadata backup file is not a file: " + path);
    }

    public String saveMetadataBackupTmpFromRequest(Long fileSize, InputStream inputStream) throws IOException {
        val tmpDirectory = Files.createTempDirectory("MetadataBackupTmp-").toFile();
        val tmpFile = new File(tmpDirectory, METADATA_FILE);
        try (val os = new FileOutputStream(tmpFile)) {
            IOUtils.copy(inputStream, os);
            val actualFileSize = tmpFile.length();
            if (actualFileSize != fileSize) {
                throw new FileNotFoundException("Metadata backup temp file length does not right: "
                        + tmpFile.getAbsolutePath() + " length :" + actualFileSize);
            }
            return tmpFile.getAbsolutePath();
        }
    }

    public void saveMetadataBackupInHDFS(String path, String tmpFilePath, Long fileSize) throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val filePath = new Path(path);
        if (fileSystem.isFile(filePath)) {
            return;
        }
        if (fileSystem.isDirectory(filePath)) {
            fileSystem.delete(filePath, true);
        }
        fileSystem.copyFromLocalFile(new Path(tmpFilePath), filePath);
        val fileStatus = fileSystem.getFileStatus(filePath);
        if (fileStatus.getLen() != fileSize) {
            throw new FileNotFoundException(
                    "Metadata backup temp file length does not right.\n Tmp file: " + tmpFilePath + " length: "
                            + fileSize + "\n DFS file: " + path + " length: " + fileStatus.getLen());
        }
    }

    public void deleteTmpDir(String fileTmpPath) {
        val tmpFile = new File(fileTmpPath);
        val tmpDir = tmpFile.getParentFile();
        try {
            FileUtils.deleteDirectory(tmpDir);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String downloadMetadataBackTmpFile(String tmpFilePath, Long tmpFileSize, String resourceGroupId,
            String fromHost) throws JsonProcessingException {
        val url = String.format(Locale.ROOT, "http://%s/kylin/api/system/metadata_backup_tmp_file", fromHost);
        val req = Maps.newHashMap();
        req.put("resource_group_id", resourceGroupId);
        req.put("tmp_file_path", tmpFilePath);
        req.put("tmp_file_size", tmpFileSize);
        val httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        httpHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM, MediaType.ALL));
        val httpEntity = new HttpEntity<>(JsonUtil.writeValueAsBytes(req), httpHeaders);
        RequestCallback requestCallback = restTemplate.httpEntityCallback(httpEntity);
        return restTemplate.execute(url, HttpMethod.POST, requestCallback, clientResponse -> {
            try (InputStream ins = clientResponse.getBody()) {
                return saveMetadataBackupTmpFromRequest(tmpFileSize, ins);
            }
        });
    }

    public void saveBroadcastMetadataBackup(String backupDir, String filePath, Long fileSize, String resourceGroupId,
            String fromHost) {
        var tmpFilePath = "";
        try {
            tmpFilePath = downloadMetadataBackTmpFile(filePath, fileSize, resourceGroupId, fromHost);
            log.info("tmpFilePath is [{}]", tmpFilePath);
            val path = StringUtils.appendIfMissing(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()),
                    BACKSLASH) + backupDir + BACKSLASH + METADATA_FILE;
            saveMetadataBackupInHDFS(path, tmpFilePath, fileSize);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new KylinRuntimeException(e);
        } finally {
            if (StringUtils.isNotBlank(tmpFilePath)) {
                deleteTmpDir(tmpFilePath);
            }
        }
    }
}
