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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

public class TimestampedRollingFileOutputDir {
    private static final Logger logger = LoggerFactory.getLogger(TimestampedRollingFileOutputDir.class);
    private final File outputDir;
    private final String fileNamePrefix;
    private final long maxFileCount;

    public TimestampedRollingFileOutputDir(File outputDir, String fileNamePrefix, long maxFileCount) {
        this.outputDir = outputDir;
        Preconditions.checkArgument(!fileNamePrefix.isEmpty(), "fileNamePrefix cannot be empty");
        Preconditions.checkArgument(maxFileCount > 0, "maxFileCount cannot be less than 1");
        this.fileNamePrefix = normalizeFileNamePrefix(fileNamePrefix);
        this.maxFileCount = maxFileCount;
    }

    public File newOutputFile() throws IOException {
        File[] files = outputDir.listFiles(fileFilter());
        Preconditions.checkNotNull(files, "Invalid output directory " + outputDir);
        logger.debug("found {} output files under output dir", files.length);
        if (files.length > 0) {
            Arrays.sort(files, fileComparatorByAge());
            removeOldFiles(files);
        }

        File newFile = new File(outputDir, newFileName());
        if (!newFile.createNewFile()) {
            logger.warn("output file {} already exists", newFile);
        }
        logger.debug("created output file {}", newFile);
        return newFile;
    }

    private String normalizeFileNamePrefix(String fileNamePrefix) {
        return fileNamePrefix.endsWith(".") ? fileNamePrefix : fileNamePrefix + ".";
    }

    protected FileFilter fileFilter() {
        return pathname -> pathname.getName().startsWith(fileNamePrefix);
    }

    protected Comparator<File> fileComparatorByAge() {
        return (file1, file2) -> file2.getName().compareTo(file1.getName());
    }

    protected void removeOldFiles(File[] files) {
        for (int i = files.length - 1; i > maxFileCount - 2; i--) {
            String filePath = files[i].getPath();
            try {
                Files.deleteIfExists(files[i].toPath());
                logger.debug("Removed outdated file {}", filePath);
            } catch (IOException e) {
                logger.error("Error removing outdated file {}", filePath, e);
            }
        }
    }

    protected String newFileName() {
        return fileNamePrefix + System.currentTimeMillis();
    }
}
