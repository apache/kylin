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

package org.apache.kylin.rest.service;

import static org.apache.kylin.query.util.AsyncQueryUtil.getUserFileName;
import static org.apache.kylin.rest.util.AclPermissionUtil.isAdmin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.exception.NAsyncQueryIllegalParamException;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;

@Component("asyncQueryService")
public class AsyncQueryService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger("query");

    public void saveQueryUsername(String project, String queryId) throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        try (FSDataOutputStream os = fileSystem.create(new Path(asyncQueryResultDir, getUserFileName()));
                OutputStreamWriter osw = new OutputStreamWriter(os, Charset.defaultCharset())) {
            osw.write(getUsername());
        }
    }

    public List<List<String>> getMetaData(String project, String queryId) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, project, MsgPicker.getMsg().getQueryResultNotFound());
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        List<List<String>> result = Lists.newArrayList();
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        try (FSDataInputStream is = fileSystem
                .open(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName()));
                BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(is, Charset.defaultCharset()))) {
            result.add(Lists.newArrayList(bufferedReader.readLine().split(",")));
            result.add(Lists.newArrayList(bufferedReader.readLine().split(",")));
        }
        return result;
    }

    public FileInfo getFileInfo(String project, String queryId) throws IOException {
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        FileInfo fileInfo = new FileInfo();
        try (FSDataInputStream is = fileSystem.open(new Path(asyncQueryResultDir, AsyncQueryUtil.getFileInfo()));
                BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(is, Charset.defaultCharset()))) {
            fileInfo.setFormat(bufferedReader.readLine());
            fileInfo.setEncode(bufferedReader.readLine());
            fileInfo.setFileName(bufferedReader.readLine());
            String sep = bufferedReader.readLine();
            fileInfo.setSeparator(sep == null ? "," : sep);
            return fileInfo;
        }
    }

    public void retrieveSavedQueryResult(String project, String queryId, boolean includeHeader,
            HttpServletResponse response, String fileFormat, String encode, String separator) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, project, MsgPicker.getMsg().getQueryResultNotFound());

        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path dataPath = getAsyncQueryResultDir(project, queryId);

        if (!fileSystem.exists(dataPath)) {
            throw new NotFoundException(MsgPicker.getMsg().getQueryResultFileNotFound());
        }

        try (ServletOutputStream outputStream = response.getOutputStream()) {
            String columnNames = null;
            if (includeHeader) {
                columnNames = processHeader(fileSystem, dataPath);
                if (columnNames != null) {
                    logger.debug("Query:{}, columnMeta:{}", columnNames, columnNames);
                    if (!columnNames.endsWith(IOUtils.LINE_SEPARATOR_UNIX)) {
                        columnNames = columnNames + IOUtils.LINE_SEPARATOR_UNIX;
                    }
                } else {
                    logger.error("Query:{}, no columnMeta found", queryId);
                }
            }
            switch (fileFormat) {
            case "csv":
                CSVWriter csvWriter = new CSVWriter();
                processCSV(outputStream, dataPath, includeHeader, columnNames, csvWriter, separator);
                break;
            case "json":
                processJSON(outputStream, dataPath, encode);
                break;
            case "xlsx":
                if (!includeHeader) {
                    processFile(outputStream, dataPath);
                } else {
                    XLSXExcelWriter xlsxExcelWriter = new XLSXExcelWriter();
                    processXLSX(outputStream, dataPath, includeHeader, columnNames, xlsxExcelWriter);
                }
                break;
            case "parquet":
                processFile(outputStream, dataPath);
                break;
            default:
                logger.info("Query:{}, processed", queryId);
            }
        }
    }

    public String retrieveSavedQueryException(String project, String queryId) throws IOException {
        Message msg = MsgPicker.getMsg();

        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        Path dataPath = new Path(getAsyncQueryResultDir(project, queryId), AsyncQueryUtil.getFailureFlagFileName());

        if (!fileSystem.exists(dataPath)) {
            throw new NotFoundException(msg.getQueryExceptionFileNotFound());
        }
        try (FSDataInputStream inputStream = fileSystem.open(dataPath);
                InputStreamReader reader = new InputStreamReader(inputStream, Charset.defaultCharset())) {
            List<String> strings = IOUtils.readLines(reader);

            return StringUtils.join(strings, "");
        }
    }

    public String searchQueryResultProject(String queryId) throws IOException {
        for (ProjectInstance projectInstance : NProjectManager.getInstance(getConfig()).listAllProjects()) {
            FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
            if (fileSystem.exists(getAsyncQueryResultDir(projectInstance.getName(), queryId))) {
                return projectInstance.getName();
            }
        }
        return null;
    }

    public QueryStatus queryStatus(String project, String queryId) throws IOException {
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        if (fileSystem.exists(asyncQueryResultDir)) {
            if (fileSystem.exists(new Path(asyncQueryResultDir, AsyncQueryUtil.getFailureFlagFileName()))) {
                return QueryStatus.FAILED;
            }
            if (fileSystem.exists(new Path(asyncQueryResultDir, AsyncQueryUtil.getSuccessFlagFileName()))
                    && fileSystem.exists(new Path(asyncQueryResultDir, AsyncQueryUtil.getFileInfo()))
                    && fileSystem.exists(new Path(asyncQueryResultDir, AsyncQueryUtil.getMetaDataFileName()))) {
                return QueryStatus.SUCCESS;
            }
            return QueryStatus.RUNNING;
        }
        return QueryStatus.MISS;
    }

    public String getQueryUsername(String queryId, String project) throws IOException {
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        Path userNamePath = new Path(asyncQueryResultDir, getUserFileName());
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        if (fileSystem.exists(asyncQueryResultDir) && fileSystem.exists(userNamePath)) {
            try (FSDataInputStream is = fileSystem.open(userNamePath);
                    BufferedReader bufferedReader = new BufferedReader(
                            new InputStreamReader(is, Charset.defaultCharset()))) {
                return bufferedReader.readLine();
            }
        }
        logger.warn("async query user name file not exist");
        return null;
    }

    public boolean hasPermission(String queryId, String project) throws IOException {
        if (getQueryUsername(queryId, project) != null && SecurityContextHolder.getContext().getAuthentication()
                .getName().equals(getQueryUsername(queryId, project))) {
            return true;
        }
        return isAdmin();
    }

    public long fileStatus(String project, String queryId) throws IOException {
        checkStatus(queryId, QueryStatus.SUCCESS, project, MsgPicker.getMsg().getQueryResultNotFound());
        Path asyncQueryResultDir = getAsyncQueryResultDir(project, queryId);
        if (AsyncQueryUtil.getFileSystem().exists(asyncQueryResultDir)
                && AsyncQueryUtil.getFileSystem().isDirectory(asyncQueryResultDir)) {
            long totalFileSize = 0;
            FileStatus[] fileStatuses = AsyncQueryUtil.getFileSystem().listStatus(asyncQueryResultDir);
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().getName().startsWith("_"))
                    totalFileSize += fileStatus.getLen();
            }
            return totalFileSize;
        } else {
            throw new NotFoundException(MsgPicker.getMsg().getQueryResultNotFound());
        }
    }

    public boolean batchDelete(String project, String time) throws IOException, ParseException {
        if (project == null && time == null) {
            return deleteAllFolder();
        } else {
            return deleteOldQueryResult(project, time);
        }
    }

    public boolean deleteAllFolder() throws IOException {
        NProjectManager projectManager = KylinConfig.getInstanceFromEnv().getManager(NProjectManager.class);
        List<ProjectInstance> projectInstances = projectManager.listAllProjects();
        boolean allSuccess = true;
        Set<Path> asyncQueryResultPaths = new HashSet<Path>();
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        for (ProjectInstance projectInstance : projectInstances) {
            Path asyncQueryResultBaseDir = getAsyncQueryResultBaseDir(projectInstance.getName());
            if (!fileSystem.exists(asyncQueryResultBaseDir)) {
                continue;
            }
            asyncQueryResultPaths.add(asyncQueryResultBaseDir);
        }

        logger.info("clean all async result dir");
        for (Path path : asyncQueryResultPaths) {
            if (!AsyncQueryUtil.getFileSystem().delete(path, true)) {
                allSuccess = false;
            }
        }
        return allSuccess;
    }

    public boolean deleteByQueryId(String project, String queryId) throws IOException {
        Path resultDir = getAsyncQueryResultDir(project, queryId);
        if (queryStatus(project, queryId) == QueryStatus.MISS) {
            throw new NAsyncQueryIllegalParamException(MsgPicker.getMsg().getQueryResultNotFound());
        }
        logger.info("clean async query result for query id [{}]", queryId);
        return AsyncQueryUtil.getFileSystem().delete(resultDir, true);
    }

    public boolean deleteOldQueryResult(String project, long time) throws IOException {
        boolean isAllSucceed = true;
        Path asyncQueryResultBaseDir = getAsyncQueryResultBaseDir(project);
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        if (!fileSystem.exists(asyncQueryResultBaseDir)) {
            return true;
        }
        FileStatus[] fileStatuses = fileSystem.listStatus(asyncQueryResultBaseDir);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.getModificationTime() < time) {
                try {
                    fileSystem.delete(fileStatus.getPath(), true);
                } catch (Exception e) {
                    logger.error("Fail to delete async query result for [{}]", fileStatus.getPath(), e);
                    isAllSucceed = false;
                }
            }
        }
        return isAllSucceed;
    }

    public boolean deleteOldQueryResult(String project, String timeString) throws IOException, ParseException {
        if (project == null || timeString == null) {
            return false;
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        long time = format.parse(timeString).getTime();
        return deleteOldQueryResult(project, time);
    }

    public boolean cleanOldQueryResult(String project, long days) throws IOException {
        return deleteOldQueryResult(project, System.currentTimeMillis() - days * 24 * 60 * 60 * 1000);
    }

    public String asyncQueryResultPath(String project, String queryId) throws IOException {
        if (queryStatus(project, queryId) == QueryStatus.MISS) {
            throw new NAsyncQueryIllegalParamException(MsgPicker.getMsg().getQueryResultNotFound());
        }
        return getAsyncQueryResultDir(project, queryId).toString();
    }

    public void checkStatus(String queryId, QueryStatus queryStatus, String project, String message)
            throws IOException {
        if (queryStatus == QueryStatus.SUCCESS && queryStatus(project, queryId) != QueryStatus.SUCCESS) {
            throw new NAsyncQueryIllegalParamException(message);
        }
    }

    public Path getAsyncQueryResultBaseDir(String project) {
        return new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(project));
    }

    public Path getAsyncQueryResultDir(String project, String queryId) {
        return new Path(KapConfig.getInstanceFromEnv().getAsyncResultBaseDir(project), queryId);
    }

    private String processHeader(FileSystem fileSystem, Path dataPath) throws IOException {

        FileStatus[] fileStatuses = fileSystem.listStatus(dataPath);
        for (FileStatus header : fileStatuses) {
            if (header.getPath().getName().equals(AsyncQueryUtil.getMetaDataFileName())) {
                try (FSDataInputStream inputStream = fileSystem.open(header.getPath());
                        BufferedReader bufferedReader = new BufferedReader(
                                new InputStreamReader(inputStream, Charset.defaultCharset()))) {
                    return bufferedReader.readLine();
                }
            }
        }
        return null;
    }

    private void processCSV(OutputStream outputStream, Path dataPath, boolean includeHeader, String columnNames,
            CSVWriter excelWriter, String separator) throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(dataPath);
        excelWriter.writeData(fileStatuses, outputStream, columnNames, separator, includeHeader);
    }

    private void processJSON(OutputStream outputStream, Path dataPath, String encode) throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(dataPath);
        List<String> rowResults = Lists.newArrayList();
        for (FileStatus f : fileStatuses) {
            if (!f.getPath().getName().startsWith("_")) {
                try (FSDataInputStream inputStream = fileSystem.open(f.getPath())) {
                    BufferedReader bufferedReader = new BufferedReader(
                            new InputStreamReader(inputStream, Charset.forName(encode)));
                    rowResults.addAll(Lists.newArrayList(bufferedReader.lines().collect(Collectors.toList())));
                }
            }
        }
        String json = new ObjectMapper().writeValueAsString(rowResults);
        IOUtils.copy(IOUtils.toInputStream(json), outputStream);
    }

    private void processFile(OutputStream outputStream, Path dataPath) throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(dataPath);
        for (FileStatus f : fileStatuses) {
            if (!f.getPath().getName().startsWith("_")) {
                try (FSDataInputStream inputStream = fileSystem.open(f.getPath())) {
                    IOUtils.copy(inputStream, outputStream);
                }
            }
        }
    }

    private void processXLSX(OutputStream outputStream, Path dataPath, boolean includeHeader, String columnNames, XLSXExcelWriter excelWriter)
            throws IOException {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(dataPath);
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet("query_result");
            // Apply column names
            if (includeHeader && columnNames != null) {
                org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(0);
                String[] columnNameArray = columnNames.split(SparderEnv.getSeparator());
                for (int i = 0; i < columnNameArray.length; i++) {
                    excelRow.createCell(i).setCellValue(columnNameArray[i]);
                }
            }
            excelWriter.writeData(fileStatuses, sheet);
            wb.write(outputStream);
        }
    }

    public enum QueryStatus {
        RUNNING, FAILED, SUCCESS, MISS
    }

    @Getter
    @Setter
    public static class FileInfo {
        private String format;
        private String encode;
        private String fileName;
        private String separator;

        public FileInfo() {
        }

        public FileInfo(String format, String encode, String fileName, String separator) {
            this.format = format;
            this.encode = encode;
            this.fileName = fileName;
            this.separator = separator;
        }

        public FileInfo(String format, String encode, String fileName) {
            this(format, encode, fileName, ",");
        }
    }
}
