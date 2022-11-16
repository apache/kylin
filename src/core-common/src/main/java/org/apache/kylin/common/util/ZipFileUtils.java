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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;

import lombok.val;

public class ZipFileUtils {

    public static void decompressZipFile(String zipFilename, String targetDir) throws IOException {
        if (!validateZipFilename(zipFilename)) {
            throw new KylinException(CommonErrorCode.INVALID_ZIP_NAME, "Zipfile must end with .zip");
        }
        String normalizedTargetDir = Paths.get(targetDir).normalize().toString();
        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilename))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                val entryDir = Paths.get(targetDir, entry.getName()).normalize().toString();
                if (!entryDir.startsWith(normalizedTargetDir)) {
                    throw new KylinException(CommonErrorCode.INVALID_ZIP_ENTRY,
                            "Zip Entry <" + entry.getName() + "> is Invalid");
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(Paths.get(entryDir));
                } else {
                    Files.createDirectories(Paths.get(entryDir).getParent());
                    Files.copy(zipInputStream, Paths.get(targetDir, entry.getName()));
                }
            }
        }
    }

    public static void compressZipFile(String sourceDir, OutputStream outputStream) throws IOException {
        ZipOutputStream zipFile = null;
        try {
            zipFile = new ZipOutputStream(outputStream);
            compressDirectoryToZipfile(normDir(new File(sourceDir).getParent()), normDir(sourceDir), zipFile);
        } finally {
            IOUtils.closeQuietly(zipFile);
        }
    }

    public static void compressZipFile(String sourceDir, String zipFilename) throws IOException {
        if (!validateZipFilename(zipFilename)) {
            throw new RuntimeException("Zipfile must end with .zip");
        }
        compressZipFile(sourceDir, new FileOutputStream(zipFilename));
    }

    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream out)
            throws IOException {
        File[] files = new File(sourceDir).listFiles();
        if (files == null)
            return;

        // should put empty directory to zip file
        if (files.length == 0) {
            out.putNextEntry(
                    new ZipEntry(normDir(StringUtils.isEmpty(rootDir) ? sourceDir : sourceDir.replace(rootDir, ""))));
        }

        for (File sourceFile : files) {
            if (sourceFile.isDirectory()) {
                compressDirectoryToZipfile(rootDir, sourceDir + normDir(sourceFile.getName()), out);
            } else {
                ZipEntry entry = new ZipEntry(
                        normDir(StringUtils.isEmpty(rootDir) ? sourceDir : sourceDir.replace(rootDir, ""))
                                + sourceFile.getName());
                entry.setTime(sourceFile.lastModified());
                out.putNextEntry(entry);
                FileInputStream in = new FileInputStream(sourceDir + sourceFile.getName());
                try {
                    IOUtils.copy(in, out);
                } finally {
                    IOUtils.closeQuietly(in);
                }
            }
        }
    }

    public static boolean validateZipFilename(String filename) {
        return !StringUtils.isEmpty(filename) && filename.trim().toLowerCase(Locale.ROOT).endsWith(".zip");
    }

    private static String normDir(String dirName) {
        if (!StringUtils.isEmpty(dirName) && !dirName.endsWith(File.separator)) {
            dirName = dirName + File.separator;
        }
        return dirName;
    }

}
