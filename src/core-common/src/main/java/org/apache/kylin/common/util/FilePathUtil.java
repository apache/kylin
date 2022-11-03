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

public class FilePathUtil {

    // Utility classes, which are collections of static members, are not meant to be instantiated.
    // Even abstract utility classes, which can be extended, should not have public constructors.
    private FilePathUtil() {
    }

    public static String completeFolderPathWithSlash(String folderPath) {
        return folderPath.endsWith("/") ? folderPath : folderPath + "/";
    }

    /**
     * Always return the empty string instead of the empty pointer
     * @param filePath Multiple possible file paths
     * @return The first file path that exists
     */
    public static String returnFilePathIfExists(String... filePath) {
        if (filePath == null) {
            return "";
        }
        for (String singleFilePath : filePath) {
            if (singleFilePath == null || singleFilePath.isEmpty()) {
                continue;
            }
            File file = new File(singleFilePath);
            if (file.exists() && file.isFile()) {
                return singleFilePath;
            }
        }
        return "";
    }
}
