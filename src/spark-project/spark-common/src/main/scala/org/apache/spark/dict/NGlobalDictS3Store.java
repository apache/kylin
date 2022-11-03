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
package org.apache.spark.dict;

import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NGlobalDictS3Store extends NGlobalDictHDFSStore {
    private static final String WORKING_PREFIX = "working_";
    private static final Logger logger = LoggerFactory.getLogger(NGlobalDictS3Store.class);

    private static ConcurrentHashMap<String, Long> pathToVersion = new ConcurrentHashMap<>();

    public NGlobalDictS3Store(String baseDir) throws IOException {
        super(baseDir);
    }

    @Override
    public void prepareForWrite(String workingDir) throws IOException {
        super.prepareForWrite(workingDir);
        Path working = new Path(workingDir);
        Path workingFlagPath = getWorkingFlagPath(basePath);
        if (fileSystem.exists(workingFlagPath)) {
            fileSystem.delete(workingFlagPath, true);
            logger.trace("Working directory {} exits, delete it first", working);
        }

        fileSystem.mkdirs(workingFlagPath);
    }

    @Override
    public Long[] listAllVersions() throws IOException {
        if (!fileSystem.exists(basePath)) {
            return new Long[0];
        }

        FileStatus[] versionDirs = fileSystem.listStatus(basePath, path -> path.getName().startsWith(VERSION_PREFIX));
        TreeSet<Long> versions = new TreeSet<>();
        for (FileStatus versionDir : versionDirs) {
            Path path = versionDir.getPath();
            long version = Long.parseLong(path.getName().substring(VERSION_PREFIX.length()));
            if (fileSystem.exists(getWorkingFlagPath(basePath, version))) {
                continue;
            }
            versions.add(version);
        }
        return versions.toArray(new Long[versions.size()]);
    }

    @Override
    public void commit(String workingDir, int maxVersions, long versionTTL) throws IOException {
        cleanWorkingFlagPath(basePath);
        logger.info("Commit {}", workingDir);
        cleanUp(maxVersions, versionTTL);
    }

    @Override
    public String getWorkingDir() {
        Path path = getWorkingFlagPath(basePath);
        long timestamp = Long.parseLong(path.getName().substring(WORKING_PREFIX.length()));
        return baseDir + VERSION_PREFIX + timestamp;
    }

    private Path getWorkingFlagPath(Path basePath) {
        long timestamp = pathToVersion.getOrDefault(basePath.toString(), System.currentTimeMillis());
        pathToVersion.putIfAbsent(basePath.toString(), timestamp);
        try {
            FileSystem fileSystem = HadoopUtil.getFileSystem(basePath);
            if (fileSystem.exists(basePath)) {
                FileStatus[] workingDirs = fileSystem.listStatus(basePath,
                        path -> path.getName().startsWith(WORKING_PREFIX));
                if (workingDirs.length > 0) {
                    Path path = workingDirs[0].getPath();
                    timestamp = Long.parseLong(path.getName().substring(WORKING_PREFIX.length()));
                }
            }
        } catch (IOException ioe) {
            logger.error("Get exception when get version", ioe);
        }
        return getWorkingFlagPath(basePath, timestamp);
    }

    private Path getWorkingFlagPath(Path basePath, long version) {
        return new Path(basePath + "/" + WORKING_PREFIX + version);
    }

    private void cleanWorkingFlagPath(Path basePath) throws IOException {
        pathToVersion.remove(basePath.toString());
        FileSystem fileSystem = HadoopUtil.getFileSystem(basePath);
        FileStatus[] workingDirs = fileSystem.listStatus(basePath, path -> path.getName().startsWith(WORKING_PREFIX));
        for (FileStatus workingDir : workingDirs) {
            logger.info("Clean working path {}", workingDir);
            fileSystem.delete(workingDir.getPath(), true);
        }
    }
}
