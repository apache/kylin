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

package org.apache.kylin.dict.lookup;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.kylin.common.util.HadoopUtil;

/**
 * @author yangli9
 * 
 */
public class FileTable implements ReadableTable {

    String path;
    String delim;
    int nColumns;
    boolean nativeTable;

    public FileTable(String path, int nColumns) {
        this(path, DELIM_AUTO, nColumns, true);
    }

    public FileTable(String path, String delim, int nColumns, boolean nativeTable) {
        this.path = path;
        this.delim = delim;
        this.nColumns = nColumns;
        this.nativeTable = nativeTable;
    }

    public FileTable(String path, int nColumns, boolean nativeTable) {
        this.path = path;
        this.delim = DELIM_AUTO;
        this.nColumns = nColumns;
        this.nativeTable = nativeTable;
    }

    @Override
    public String getColumnDelimeter() {
        return delim;
    }

    @Override
    public TableReader getReader() throws IOException {
        return new FileTableReader(path, delim, nColumns);
    }

    @Override
    public TableSignature getSignature() throws IOException {
        FileSystem fs = HadoopUtil.getFileSystem(path);
        FileStatus status = fs.getFileStatus(new Path(path));
        if (nativeTable) {
            return new TableSignature(path, status.getLen(), status.getModificationTime());
        }
        return new TableSignature(path, status.getLen(), System.currentTimeMillis());
    }

    @Override
    public String toString() {
        return path;
    }
}
