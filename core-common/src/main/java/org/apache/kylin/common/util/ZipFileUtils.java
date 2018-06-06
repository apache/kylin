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

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.examples.Archiver;
import org.apache.commons.compress.archivers.examples.Expander;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipFileUtils {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileUtils.class);

    public static void compressZipFile(String sourceDir, String zipFileName) throws IOException, ArchiveException {
        if (!validateZipFilename(zipFileName)) {
            throw new RuntimeException("Zip file must end with .zip");
        }
        Archiver archiver = new Archiver();
        archiver.create(ArchiveStreamFactory.ZIP, new File(zipFileName), new File(sourceDir));
    }

    public static void decompressZipfileToDirectory(String zipFileName, File outputFolder)
            throws IOException, ArchiveException {
        if (!validateZipFilename(zipFileName)) {
            throw new RuntimeException("Zip file must end with .zip");
        }
        Expander expander = new Expander();
        ZipFile zipFile = new ZipFile(zipFileName);
        expander.expand(zipFile, outputFolder);
    }

    private static boolean validateZipFilename(String filename) {
        return !StringUtils.isEmpty(filename) && filename.trim().toLowerCase().endsWith(".zip");
    }
}
