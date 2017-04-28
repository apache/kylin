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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipFileUtils {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileUtils.class);

    public static void compressZipFile(String sourceDir, String zipFilename) throws IOException {
        if (!validateZipFilename(zipFilename)) {
            throw new RuntimeException("Zipfile must end with .zip");
        }
        ZipOutputStream zipFile = null;
        try {
            zipFile = new ZipOutputStream(new FileOutputStream(zipFilename));
            compressDirectoryToZipfile(normDir(new File(sourceDir).getParent()), normDir(sourceDir), zipFile);
        } finally {
            IOUtils.closeQuietly(zipFile);
        }
    }

    public static void decompressZipfileToDirectory(String zipFileName, File outputFolder) throws IOException {
        ZipInputStream zipInputStream = null;
        try {
            zipInputStream = new ZipInputStream(new FileInputStream(zipFileName));
            ZipEntry zipEntry = null;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                logger.info("decompressing " + zipEntry.getName() + " is directory:" + zipEntry.isDirectory() + " available: " + zipInputStream.available());

                File temp = new File(outputFolder, zipEntry.getName());
                if (zipEntry.isDirectory()) {
                    temp.mkdirs();
                } else {
                    temp.getParentFile().mkdirs();
                    temp.createNewFile();
                    temp.setLastModified(zipEntry.getTime());
                    FileOutputStream outputStream = new FileOutputStream(temp);
                    try {
                        IOUtils.copy(zipInputStream, outputStream);
                    } finally {
                        IOUtils.closeQuietly(outputStream);
                    }
                }
            }
        } finally {
            IOUtils.closeQuietly(zipInputStream);
        }
    }

    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream out) throws IOException {
        File[] files = new File(sourceDir).listFiles();
        if (files == null)
            return;
        for (File sourceFile : files) {
            if (sourceFile.isDirectory()) {
                compressDirectoryToZipfile(rootDir, sourceDir + normDir(sourceFile.getName()), out);
            } else {
                ZipEntry entry = new ZipEntry(normDir(StringUtils.isEmpty(rootDir) ? sourceDir : sourceDir.replace(rootDir, "")) + sourceFile.getName());
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

    private static boolean validateZipFilename(String filename) {
        if (!StringUtils.isEmpty(filename) && filename.trim().toLowerCase().endsWith(".zip")) {
            return true;
        }

        return false;
    }

    private static String normDir(String dirName) {
        if (!StringUtils.isEmpty(dirName) && !dirName.endsWith(File.separator)) {
            dirName = dirName + File.separator;
        }
        return dirName;
    }
}
