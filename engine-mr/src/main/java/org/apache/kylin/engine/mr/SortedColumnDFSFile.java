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
package org.apache.kylin.engine.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.dict.ByteComparator;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.source.ReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiefan on 16-11-22.
 *
 * Read values from multi col files and ensure their order using a K-Way merge algorithm
 *
 * You need to ensure that values inside each file is sorted
 */
public class SortedColumnDFSFile implements ReadableTable {

    private static final Logger logger = LoggerFactory.getLogger(SortedColumnDFSFile.class);

    private String dfsPath;

    private DFSFileTable dfsFileTable;

    private DataType dataType;

    public SortedColumnDFSFile(String path, DataType dataType) {
        this.dfsPath = path;
        this.dfsFileTable = new DFSFileTable(path, -1);
        this.dataType = dataType;
    }

    @Override
    public TableReader getReader() throws IOException {
        final Comparator<String> comparator = getComparatorByType(dataType);

        ArrayList<TableReader> readers = new ArrayList<>();
        String filePath = HadoopUtil.fixWindowsPath(dfsPath);
        FileSystem fs = HadoopUtil.getFileSystem(filePath);
        ArrayList<FileStatus> allFiles = new ArrayList<>();
        FileStatus status = fs.getFileStatus(new Path(filePath));
        if (status.isFile()) {
            allFiles.add(status);
        } else {
            FileStatus[] listStatus = fs.listStatus(new Path(filePath));
            for (FileStatus f : listStatus) {
                if (f.isFile())
                    allFiles.add(f);
            }
        }
        for (FileStatus f : allFiles) {
            DFSFileTableReader reader = new DFSFileTableReader(f.getPath().toString(), -1);
            readers.add(reader);
        }

        return new SortedColumnDFSFileReader(readers, comparator);
    }

    @Override
    public TableSignature getSignature() throws IOException {
        return dfsFileTable.getSignature();
    }
    
    @Override
    public boolean exists() throws IOException {
        return dfsFileTable.exists();
    }

    private Comparator<String> getComparatorByType(DataType type) {
        Comparator<String> comparator;
        if (!type.isNumberFamily()) {
            comparator = new ByteComparator<>(new StringBytesConverter());
        } else if (type.isIntegerFamily()) {
            comparator = new Comparator<String>() {
                @Override
                public int compare(String str1, String str2) {
                    try {
                        Long num1 = Long.parseLong(str1);
                        Long num2 = Long.parseLong(str2);
                        return num1.compareTo(num2);
                    } catch (NumberFormatException e) {
                        logger.error("NumberFormatException when parse integer family number.str1:" + str1 + " str2:" + str2);
                        e.printStackTrace();
                        return 0;
                    }
                }
            };
        } else {
            comparator = new Comparator<String>() {
                @Override
                public int compare(String str1, String str2) {
                    try {
                        Double num1 = Double.parseDouble(str1);
                        Double num2 = Double.parseDouble(str2);
                        return num1.compareTo(num2);
                    } catch (NumberFormatException e) {
                        logger.error("NumberFormatException when parse doul family number.str1:" + str1 + " str2:" + str2);
                        return 0;
                    }
                }
            };
        }
        return comparator;
    }

    @Override
    public String toString() {
        return dfsPath;
    }
}
