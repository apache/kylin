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

package org.apache.kylin.engine.mr.streaming;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.stream.core.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ColumnarFilesReader {
    private static final Logger logger = LoggerFactory.getLogger(ColumnarFilesReader.class);
    protected Path folderPath;
    protected FileSystem fs;
    protected Path dataFilePath;
    protected Path metaFilePath;

    public ColumnarFilesReader(FileSystem fs, Path folderPath) {
        this.fs = fs;
        this.folderPath = folderPath;
        checkPath();
    }

    void checkPath() {
        try {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(folderPath, false);
            if (files == null) {
                throw new IllegalArgumentException("Invalid path " + folderPath);
            }
            while (files.hasNext()) {
                LocatedFileStatus fileStatus = files.next();
                Path path = fileStatus.getPath();
                String name = path.getName();

                if (name.endsWith(Constants.DATA_FILE_SUFFIX)) {
                    dataFilePath = path;
                } else if (name.endsWith(Constants.META_FILE_SUFFIX)) {
                    metaFilePath = path;
                } else {
                    logger.warn("Contains invalid file {} in path {}", path, folderPath);
                }
            }
            if (dataFilePath == null || metaFilePath == null) {
                throw new IllegalArgumentException("Invalid path " + folderPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("io error", e);
        }
    }

    public abstract void close() throws IOException;

}
