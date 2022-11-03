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

package org.apache.kylin.source.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.source.IReadableTable;

/**
 */
public class DFSFileTable implements IReadableTable {

    public static final String DELIM_AUTO = "auto";
    public static final String DELIM_COMMA = ",";

    String path;
    String delim;
    int nColumns;

    public DFSFileTable(String path, int nColumns) {
        this(path, DELIM_AUTO, nColumns);
    }

    public DFSFileTable(String path, String delim, int nColumns) {
        this.path = path;
        this.delim = delim;
        this.nColumns = nColumns;
    }

    public String getColumnDelimeter() {
        return delim;
    }

    @Override
    public TableReader getReader() throws IOException {
        return new DFSFileTableReader(path, delim, nColumns);
    }

    @Override
    public TableSignature getSignature() throws IOException {
        Pair<Long, Long> sizeAndLastModified;
        try {
            sizeAndLastModified = getSizeAndLastModified(path);
        } catch (FileNotFoundException ex) {
            sizeAndLastModified = Pair.newPair(-1L, 0L);
        }
        return new TableSignature(path, sizeAndLastModified.getFirst(), sizeAndLastModified.getSecond());
    }

    @Override
    public boolean exists() throws IOException {
        try {
            getSizeAndLastModified(path);
            return true;
        } catch (FileNotFoundException ex) {
            return false;
        }
    }

    @Override
    public String toString() {
        return path;
    }

    public static Pair<Long, Long> getSizeAndLastModified(String path) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        // get all contained files if path is directory
        ArrayList<FileStatus> allFiles = new ArrayList<>();
        FileStatus status = fs.getFileStatus(new Path(path));
        if (status.isFile()) {
            allFiles.add(status);
        } else {
            FileStatus[] listStatus = fs.listStatus(new Path(path));
            allFiles.addAll(Arrays.asList(listStatus));
        }

        long size = 0;
        long lastModified = 0;
        for (FileStatus file : allFiles) {
            size += file.getLen();
            lastModified = Math.max(lastModified, file.getModificationTime());
        }

        return Pair.newPair(size, lastModified);
    }

}
