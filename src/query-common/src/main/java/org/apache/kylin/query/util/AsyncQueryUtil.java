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

package org.apache.kylin.query.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.exception.NAsyncQueryIllegalParamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class AsyncQueryUtil {
    public static final String ASYNC_QUERY_JOB_ID_PRE = "ASYNC-QUERY-";
    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryUtil.class);

    private AsyncQueryUtil() {
    }

    public static FileSystem getFileSystem() {
        return HadoopUtil.getWorkingFileSystem();
    }

    public static void saveMetaDataAndFileInfo(QueryContext queryContext, List<SelectedColumnMeta> columnMetas) {
        try {
            saveMetaData(queryContext.getProject(), columnMetas, queryContext.getQueryId());
            saveFileInfo(queryContext.getProject(), queryContext.getQueryTagInfo().getFileFormat(),
                    queryContext.getQueryTagInfo().getFileEncode(), queryContext.getQueryTagInfo().getFileName(),
                    queryContext.getQueryId(), queryContext.getQueryTagInfo().getSeparator());
        } catch (IOException e) {
            logger.error("save async query column metadata or file info failed.", e);
        }
    }

    public static void saveMetaData(String project, List<SelectedColumnMeta> columnMetas, String queryId)
            throws IOException {
        ArrayList<String> dataTypes = Lists.newArrayList();
        ArrayList<String> columnNames = Lists.newArrayList();
        for (SelectedColumnMeta selectedColumnMeta : columnMetas) {
            dataTypes.add(selectedColumnMeta.getColumnTypeName());
            columnNames.add(selectedColumnMeta.getName());
        }

        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        if (fileSystem.exists(asyncQueryResultDir)) {
            try (FSDataOutputStream os = getFileSystem().create(new Path(asyncQueryResultDir, getMetaDataFileName())); //
                    OutputStreamWriter osw = new OutputStreamWriter(os, Charset.defaultCharset())) {
                String metaString = StringUtils.join(columnNames, ",") + "\n" + StringUtils.join(dataTypes, ",");
                osw.write(metaString);
            }
        } else {
            throw new NAsyncQueryIllegalParamException(MsgPicker.getMsg().getQueryResultNotFound());
        }
    }

    public static void saveFileInfo(String project, String format, String encode, String fileName, String queryId,
            String separator) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        if (fileSystem.exists(asyncQueryResultDir)) {
            try (FSDataOutputStream os = getFileSystem().create(new Path(asyncQueryResultDir, getFileInfo())); //
                    OutputStreamWriter osw = new OutputStreamWriter(os, Charset.defaultCharset())) {
                osw.write(format + "\n");
                osw.write(encode + "\n");
                osw.write(fileName + "\n");
                osw.write(separator);
            }
        } else {
            throw new NAsyncQueryIllegalParamException(MsgPicker.getMsg().getQueryResultNotFound());
        }
    }

    public static void createErrorFlag(String project, String queryId, String errorMessage) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        if (!fileSystem.exists(asyncQueryResultDir)) {
            fileSystem.mkdirs(asyncQueryResultDir);
        }
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getFailureFlagFileName())); //
                OutputStreamWriter osw = new OutputStreamWriter(os, Charset.defaultCharset())) {
            if (errorMessage != null) {
                osw.write(errorMessage);
                os.hflush();
            }
        }
    }

    public static void createSuccessFlag(String project, String queryId) throws IOException {
        FileSystem fileSystem = getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getSuccessFlagFileName()))) {
            os.hflush();
        }
    }

    public static Path getAsyncQueryResultDir(String project, String queryId) {
        return new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(project), queryId);
    }

    public static String getSuccessFlagFileName() {
        return "_SUCCESS";
    }

    public static String getFailureFlagFileName() {
        return "_FAILED";
    }

    public static String getMetaDataFileName() {
        return "_METADATA";
    }

    public static String getUserFileName() {
        return "_USER";
    }

    public static String getFileInfo() {
        return "_FILEINFO";
    }

}
