/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.dict.lookup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.common.util.CliCommandExecutor;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author yangli9
 * 
 */
public class HiveTable implements ReadableTable {

    private static final Logger logger = LoggerFactory.getLogger(HiveTable.class);

    private String hiveTable;
    private int nColumns;
    private String hdfsLocation;
    private FileTable fileTable;

    public HiveTable(MetadataManager metaMgr, String table) {
        this.hiveTable = table;
        this.nColumns = metaMgr.getTableDesc(table).getColumnCount();
    }

    @Override
    public String getColumnDelimeter() throws IOException {
        return getFileTable().getColumnDelimeter();
    }

    @Override
    public TableReader getReader() throws IOException {
        return getFileTable().getReader();
    }

    @Override
    public TableSignature getSignature() throws IOException {
        return getFileTable().getSignature();
    }

    private FileTable getFileTable() throws IOException {
        if (fileTable == null) {
            fileTable = new FileTable(getHDFSLocation(true), nColumns);
        }
        return fileTable;
    }

    public String getHDFSLocation(boolean needFilePath) throws IOException {
        if (hdfsLocation == null) {
            hdfsLocation = computeHDFSLocation(needFilePath);
        }
        return hdfsLocation;
    }

    private String computeHDFSLocation(boolean needFilePath) throws IOException {

        String override = KylinConfig.getInstanceFromEnv().getOverrideHiveTableLocation(hiveTable);
        if (override != null) {
            logger.debug("Override hive table location " + hiveTable + " -- " + override);
            return override;
        }

        String cmd = "hive -e \"describe extended " + hiveTable + ";\"";
        CliCommandExecutor exec = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        String output = exec.execute(cmd);

        Pattern ptn = Pattern.compile("location:(.*?),");
        Matcher m = ptn.matcher(output);
        if (m.find() == false)
            throw new IOException("Failed to find HDFS location for hive table " + hiveTable + " from output -- " + output);

        String hdfsDir = m.group(1);

        if (needFilePath) {
            FileSystem fs = HadoopUtil.getFileSystem(hdfsDir);
            FileStatus file = findOnlyFile(hdfsDir, fs);
            return file.getPath().toString();
        } else {
            return hdfsDir;
        }
    }

    private FileStatus findOnlyFile(String hdfsDir, FileSystem fs) throws FileNotFoundException, IOException {
        FileStatus[] files = fs.listStatus(new Path(hdfsDir));
        ArrayList<FileStatus> nonZeroFiles = Lists.newArrayList();
        for (FileStatus f : files) {
            if (f.getLen() > 0)
                nonZeroFiles.add(f);
        }
        if (nonZeroFiles.size() != 1)
            throw new IllegalStateException("Expect 1 and only 1 non-zero file under " + hdfsDir + ", but find " + nonZeroFiles.size());
        return nonZeroFiles.get(0);
    }

    @Override
    public String toString() {
        return "hive:" + hiveTable;
    }

}
