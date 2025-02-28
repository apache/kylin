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
package org.apache.kylin.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHistoryOffsetTool extends CancelableTask {
    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String QUERY_HISTORY_DIR = "query_history_id_offset";
    private static final String ZIP_SUFFIX = ".zip";

    public void backup(String dir, String project) throws IOException {
        extractToHDFS(dir + "/" + QUERY_HISTORY_DIR, project);
    }

    public void restore(String dir, boolean isTruncate) throws IOException {
        Path path = new Path(dir + "/" + QUERY_HISTORY_DIR);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        for (FileStatus fileStatus : fs.listStatus(path)) {
            String fileName = fileStatus.getPath().getName();
            String project = fileName.substring(0, fileName.indexOf("."));
            restoreProject(dir, project, isTruncate);
        }
    }

    public void restoreProject(String dir, String project, boolean isTruncate) throws IOException {
        Path path = new Path(dir + "/" + QUERY_HISTORY_DIR + "/" + project + ZIP_SUFFIX);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        List<QueryHistoryIdOffset> offsets = Lists.newArrayList();
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(project);
        try (ZipInputStream zis = new ZipInputStream(fs.open(path));
                BufferedReader br = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
            while (zis.getNextEntry() != null) {
                String value = br.readLine();
                QueryHistoryIdOffset offset = JsonUtil.readValue(value, QueryHistoryIdOffset.class);
                offset.setProject(project);
                offsets.add(offset);
            }
        }
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            if (isTruncate) {
                manager.delete();
            }
            for (QueryHistoryIdOffset offset : offsets) {
                manager.updateWithoutMvccCheck(offset);
            }
            return null;
        });
    }

    public void extractFull(File dir) throws IOException {
        List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects();
        for (ProjectInstance project : projects) {
            extractProject(dir, project.getName());
        }
    }

    public void extractProject(File dir, String project) throws IOException {
        File projectFile = new File(dir, project);
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(project);
        try (OutputStream os = Files.newOutputStream(projectFile.toPath());
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            for (QueryHistoryIdOffset.OffsetType type : QueryHistoryIdOffsetManager.ALL_OFFSET_TYPE) {
                QueryHistoryIdOffset offset = manager.get(type);
                try {
                    bw.write(JsonUtil.writeValueAsString(offset));
                    bw.newLine();
                    if (isCanceled()) {
                        logger.info("query history off set backup was canceled.");
                        return;
                    }
                } catch (Exception e) {
                    logger.error("Write error, id is {}", offset.getId(), e);
                }
            }
        }
    }

    public void extractToHDFS(String dir, String project) throws IOException {
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String filePathStr = dir + "/" + project + ZIP_SUFFIX;
        try (FSDataOutputStream fos = fs.create(new Path(filePathStr));
                ZipOutputStream zos = new ZipOutputStream(fos);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zos, Charset.defaultCharset()))) {
            for (QueryHistoryIdOffset.OffsetType type : QueryHistoryIdOffsetManager.ALL_OFFSET_TYPE) {
                QueryHistoryIdOffset offset = manager.get(type);
                String pathStr = offset.getId() + "_" + type.getName() + ".json";
                zos.putNextEntry(new ZipEntry(pathStr));
                bw.write(JsonUtil.writeValueAsString(offset));
                bw.flush();
            }
        }
    }

    public void backupToLocal(String dir, String project) throws IOException {
        String extractDir = prepareLocalBackupDir(dir, project);
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(project);
        for (QueryHistoryIdOffset.OffsetType type : QueryHistoryIdOffsetManager.ALL_OFFSET_TYPE) {
            QueryHistoryIdOffset offset = manager.get(type);
            String pathname = extractDir + offset.getId() + "_" + type.getName() + ".json";
            try (BufferedWriter bf = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(new File(pathname).toPath()), StandardCharsets.UTF_8))) {
                bf.write(JsonUtil.writeValueAsString(offset));
                bf.flush();
            }
        }
    }

    private String prepareLocalBackupDir(String dir, String project) throws IOException {
        File rootDir = new File(dir);
        if (!rootDir.exists()) {
            FileUtils.forceMkdir(rootDir);
        }
        String queryHistoryOffsetDirPath = StringUtils.appendIfMissing(dir, "/") + QUERY_HISTORY_DIR;
        File queryHistoryOffsetDir = new File(queryHistoryOffsetDirPath);
        if (!queryHistoryOffsetDir.exists()) {
            FileUtils.forceMkdir(queryHistoryOffsetDir);
        }
        String extractDirPath = queryHistoryOffsetDirPath + "/" + project + "/";
        File extractDir = new File(extractDirPath);
        if (!extractDir.exists()) {
            FileUtils.forceMkdir(extractDir);
        }
        return extractDirPath;
    }

    public void restoreFromLocal(String dir, boolean isTruncate) throws IOException {
        String restorePath = dir + "/" + QUERY_HISTORY_DIR;
        File restoreDir = new File(restorePath);
        if (!restoreDir.exists()) {
            return;
        }
        File[] projectDirs = restoreDir.listFiles();
        for (File projectDir : Objects.requireNonNull(projectDirs)) {
            String project = projectDir.getName();
            restoreProjectFromLocal(dir, project, isTruncate);
        }
    }

    public void restoreProjectFromLocal(String dir, String project, boolean isTruncate) throws IOException {
        String restoreProjectPath = dir + "/" + QUERY_HISTORY_DIR + "/" + project;
        File restoreProjectDir = new File(restoreProjectPath);
        if (!restoreProjectDir.exists()) {
            return;
        }
        File[] jsonFiles = restoreProjectDir.listFiles();
        List<QueryHistoryIdOffset> offsets = Lists.newArrayList();
        QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(project);
        for (File jsonFile : Objects.requireNonNull(jsonFiles)) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(Files.newInputStream(jsonFile.toPath()), StandardCharsets.UTF_8))) {
                String value = br.readLine();
                QueryHistoryIdOffset offset = JsonUtil.readValue(value, QueryHistoryIdOffset.class);
                offset.setProject(project);
                offsets.add(offset);
            }
        }
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            if (isTruncate) {
                manager.delete();
            }
            for (QueryHistoryIdOffset offset : offsets) {
                manager.updateWithoutMvccCheck(offset);
            }
            return null;
        });
    }

}
