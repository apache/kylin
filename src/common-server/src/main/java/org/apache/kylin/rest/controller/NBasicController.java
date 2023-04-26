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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.guava30.shaded.common.net.HttpHeaders.ACCEPT_ENCODING;
import static org.apache.kylin.guava30.shaded.common.net.HttpHeaders.CONTENT_DISPOSITION;
import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_ID;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CONNECT_CATALOG;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DOWNLOAD_FILE;
import static org.apache.kylin.common.exception.ServerErrorCode.INCORRECT_PROJECT_MODE;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ARGS_TYPE_CHECK;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.DATETIME_FORMAT_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.DATETIME_FORMAT_PARSE_ERROR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.INTEGER_NON_NEGATIVE_CHECK;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_CONFLICT_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_IN_RANGE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_LESS_THAN_ZERO;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_UNAUTHORIZED;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.request.Validation;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.util.DataRangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.SneakyThrows;
import lombok.val;

public class NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NBasicController.class);
    public static final int MAX_NAME_LENGTH = 50;

    public static final long FIVE_MINUTE_MILLISECOND = TimeUnit.MINUTES.toMillis(5);

    public static final long THIRTY_DAYS_MILLISECOND = TimeUnit.DAYS.toMillis(30);

    @Autowired
    @Qualifier("normalRestTemplate")
    private RestTemplate restTemplate;

    @Autowired
    private ProjectService projectService;

    @Autowired
    protected UserService userService;

    protected Logger getLogger() {
        return logger;
    }

    public ProjectInstance getProject(String project) {
        if (null != project) {
            List<ProjectInstance> projectInstanceList = projectService.getReadableProjects(project, true);
            if (CollectionUtils.isNotEmpty(projectInstanceList)) {
                return projectInstanceList.get(0);
            }
        }

        throw new KylinException(PROJECT_NOT_EXIST, project);
    }

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(Exception.class)
    @ResponseBody
    ErrorResponse handleError(HttpServletRequest req, Throwable ex) {
        getLogger().error("", ex);
        Message msg = MsgPicker.getMsg();
        Throwable cause = ex;
        KylinException kylinException = null;
        while (cause != null && cause.getCause() != null) {
            if (cause instanceof CannotCreateTransactionException) {
                kylinException = new KylinException(FAILED_CONNECT_CATALOG, msg.getConnectDatabaseError(), false);
            }
            if (cause instanceof KylinException) {
                kylinException = (KylinException) cause;
            }
            cause = cause.getCause();
        }
        if (kylinException != null) {
            cause = kylinException;
        }
        return new ErrorResponse(req.getRequestURL().toString(), cause);
    }

    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ExceptionHandler(ForbiddenException.class)
    @ResponseBody
    ErrorResponse handleForbidden(HttpServletRequest req, Exception ex) {
        getLogger().error("", ex);
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(NotFoundException.class)
    @ResponseBody
    ErrorResponse handleNotFound(HttpServletRequest req, Exception ex) {
        getLogger().error("", ex);
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(TransactionException.class)
    @ResponseBody
    ErrorResponse handleTransaction(HttpServletRequest req, Throwable ex) {
        getLogger().error("", ex);
        Throwable root = ExceptionUtils.getRootCause(ex) == null ? ex : ExceptionUtils.getRootCause(ex);
        if (root instanceof AccessDeniedException) {
            return handleAccessDenied(req, root);
        } else if (root instanceof KylinException) {
            return handleErrorCode(req, root);
        } else {
            return handleError(req, ex);//use ex , not root
        }
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(AccessDeniedException.class)
    @ResponseBody
    ErrorResponse handleAccessDenied(HttpServletRequest req, Throwable ex) {
        getLogger().error("", ex);
        KylinException e = new KylinException(ACCESS_DENIED, MsgPicker.getMsg().getAccessDeny());
        return new ErrorResponse(req.getRequestURL().toString(), e);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({ MethodArgumentTypeMismatchException.class, MissingServletRequestParameterException.class,
            IllegalArgumentException.class })
    @ResponseBody
    ErrorResponse handleInvalidRequestParam(HttpServletRequest req, Throwable ex) {
        KylinException e = new KylinException(INVALID_PARAMETER, ex);
        getLogger().error("", e);
        return new ErrorResponse(req.getRequestURL().toString(), e);
    }

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(KylinException.class)
    @ResponseBody
    ErrorResponse handleErrorCode(HttpServletRequest req, Throwable ex) {
        getLogger().error("", ex);
        KylinException cause = (KylinException) ex;
        return new ErrorResponse(req.getRequestURL().toString(), cause);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    ErrorResponse handleInvalidArgument(HttpServletRequest request, MethodArgumentNotValidException ex) {
        val response = new ErrorResponse(request.getRequestURL().toString(), ex);
        val target = ex.getBindingResult().getTarget();
        if (target instanceof Validation) {
            response.setMsg(((Validation) target).getErrorMessage(ex.getBindingResult().getFieldErrors()));
        } else {
            response.setMsg(ex.getBindingResult().getFieldErrors().stream()
                    .map(e -> e.getField() + ":" + e.getDefaultMessage()).collect(Collectors.joining(",")));
        }

        return response;
    }

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(UnauthorizedException.class)
    @ResponseBody
    ErrorResponse handleUnauthorized(HttpServletRequest req, Throwable ex) {
        KylinException e = new KylinException(USER_UNAUTHORIZED, ex);
        getLogger().error("", e);
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    protected void checkRequiredArg(String fieldName, Object fieldValue) {
        if (fieldValue == null || StringUtils.isEmpty(String.valueOf(fieldValue))) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, fieldName);
        }
    }

    protected void checkListRequiredArg(String fieldName, Collection<?> fieldValue) {
        if (CollectionUtils.isEmpty(fieldValue)) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, fieldName);
        }
    }

    public String makeUserNameCaseInSentive(String userName) {
        UserDetails userDetails = userService.loadUserByUsername(userName);
        if (userDetails == null) {
            return userName;
        }
        return userDetails.getUsername();
    }

    public List<String> makeUserNameCaseInSentive(List<String> userNames) {
        if (CollectionUtils.isNotEmpty(userNames)) {
            return userNames.stream().map(this::makeUserNameCaseInSentive).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    protected void checkNonNegativeIntegerArg(String fieldName, Object fieldValue) {
        checkRequiredArg(fieldName, fieldValue);
        try {
            int i = Integer.parseInt(String.valueOf(fieldValue));
            if (i < 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new KylinException(INTEGER_NON_NEGATIVE_CHECK, fieldName);
        }
    }

    protected void checkBooleanArg(String fieldName, Object fieldValue) {
        checkRequiredArg(fieldName, fieldValue);
        String booleanString = String.valueOf(fieldValue);
        if (!"true".equalsIgnoreCase(booleanString) && !"false".equalsIgnoreCase(booleanString)) {
            throw new KylinException(ARGS_TYPE_CHECK, booleanString, "Boolean");
        }
    }

    protected void setDownloadResponse(File file, String fileName, String contentType,
            final HttpServletResponse response) {
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            setDownloadResponse(fileInputStream, fileName, contentType, response);
            response.setContentLength((int) file.length());
        } catch (IOException e) {
            throw new KylinException(FAILED_DOWNLOAD_FILE, e);
        }
    }

    protected void setDownloadResponse(InputStream inputStream, String fileName, String contentType,
            final HttpServletResponse response) {
        try (OutputStream output = response.getOutputStream()) {
            response.reset();
            response.setContentType(contentType);
            response.setHeader(CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            IOUtils.copyLarge(inputStream, output);
            output.flush();
        } catch (IOException e) {
            throw new KylinException(FAILED_DOWNLOAD_FILE, e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
                //empty
            }
        }
    }

    protected void setDownloadResponse(String downloadFile, String contentType, final HttpServletResponse response) {
        File file = new File(downloadFile);
        setDownloadResponse(file, file.getName(), contentType, response);
    }

    public static boolean isAdmin() {
        boolean isAdmin = false;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null) {
            for (GrantedAuthority auth : authentication.getAuthorities()) {
                if (auth.getAuthority().equals(Constant.ROLE_ADMIN)) {
                    isAdmin = true;
                    break;
                }
            }
        }
        return isAdmin;
    }

    public Map<String, Object> getDataResponse(String name, List<?> result, int offset, int limit) {
        Map<String, Object> data = new HashMap<>();
        data.put(name, PagingUtil.cutPage(result, offset, limit));
        data.put("size", result.size());
        return data;
    }

    public Map<String, Object> setCustomDataResponse(String name, Pair<List<TableDesc>, Integer> result, int offset, int limit) {
        Map<String, Object> data = new HashMap<>();
        data.put(name, PagingUtil.cutPage(result.getFirst(), offset, limit));
        data.put("size", result.getSecond());
        return data;
    }

    public String getHost(String serverHost, String serverName) {
        String host = KylinConfig.getInstanceFromEnv().getModelExportHost();
        return Optional.ofNullable(Optional.ofNullable(host).orElse(serverHost)).orElse(serverName);
    }

    public int getPort(Integer serverPort, Integer requestServerPort) {
        Integer port = KylinConfig.getInstanceFromEnv().getModelExportPort() == -1 ? null
                : KylinConfig.getInstanceFromEnv().getModelExportPort();
        return Optional.ofNullable(Optional.ofNullable(port).orElse(serverPort)).orElse(requestServerPort);
    }

    public String checkProjectName(String project) {
        if (StringUtils.isEmpty(project)) {
            throw new KylinException(EMPTY_PROJECT_NAME, MsgPicker.getMsg().getEmptyProjectName());
        }
        return getProjectNameFromEnv(project);
    }

    @SneakyThrows
    public void checkParamLength(String paramName, Object param, int length) {
        if (param == null) {
            return;
        }
        String paramStr = JsonUtil.writeValueAsString(param);
        if (paramStr.length() * 2 > length) {
            throw new KylinException(INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getParamTooLarge(), paramName, length));
        }
    }

    public static void checkSqlIsNotNull(List<String> rawSqls) {
        if (CollectionUtils.isEmpty(rawSqls)) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getSqlListIsEmpty());
        }
    }

    protected void checkSegmentParms(String[] ids, String[] names) {

        //both not empty
        if (ArrayUtils.isNotEmpty(ids) && ArrayUtils.isNotEmpty(names)) {
            throw new KylinException(SEGMENT_CONFLICT_PARAMETER);
        }

        //both empty
        if (ArrayUtils.isEmpty(ids) && ArrayUtils.isEmpty(names)) {
            throw new KylinException(SEGMENT_EMPTY_PARAMETER);
        }
    }

    // Invoke this method after checkProjectName(), otherwise NPE will happen
    public void checkProjectNotSemiAuto(String project) {
        if (!NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).isSemiAutoMode()) {
            throw new KylinException(INCORRECT_PROJECT_MODE, MsgPicker.getMsg().getProjectUnmodifiableReason());
        }
    }

    // Invoke this method after checkProjectName(), otherwise NPE will happen
    public void checkProjectUnmodifiable(String project) {
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).isExpertMode()) {
            throw new KylinException(INCORRECT_PROJECT_MODE, MsgPicker.getMsg().getProjectUnmodifiableReason());
        }
    }

    public <T extends Enum> List<String> formatStatus(List<String> status, Class<T> enumClass) {
        if (status == null) {
            return Lists.newArrayList();
        }
        Set<String> enumStrSet = Arrays.stream(enumClass.getEnumConstants()).map(Objects::toString)
                .collect(Collectors.toSet());
        List<String> formattedStatus = status.stream().filter(Objects::nonNull)
                .map(item -> item.toUpperCase(Locale.ROOT)).collect(Collectors.toList());

        List<String> illegalStatus = formattedStatus.stream().filter(item -> !enumStrSet.contains(item))
                .collect(Collectors.toList());

        if (!illegalStatus.isEmpty()) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "status", "ONLINE, OFFLINE, WARNING, BROKEN");
        }
        return formattedStatus;
    }

    public static void checkId(String uuid) {
        if (StringUtils.isEmpty(uuid)) {
            throw new KylinException(EMPTY_ID, MsgPicker.getMsg().getIdCannotEmpty());
        }
    }

    public static void validatePriority(int priority) {
        if (!ExecutablePO.isPriorityValid(priority)) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "priority", "0, 1, 2, 3, 4");
        }
    }

    public void validateRange(String start, String end) {
        validateRange(Long.parseLong(start), Long.parseLong(end));
    }

    private void validateRange(long start, long end) {
        if (start < 0 || end < 0) {
            throw new KylinException(TIME_INVALID_RANGE_LESS_THAN_ZERO);
        }
        if (start >= end) {
            throw new KylinException(TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START);
        }
    }

    public void validateDataRange(String start, String end) {
        DataRangeUtils.validateDataRange(start, end, null);
        if (StringUtils.isNotEmpty(start) && StringUtils.isNotEmpty(end)) {
            long differenceMillisecond = Long.parseLong(end) - Long.parseLong(start);
            if (differenceMillisecond < FIVE_MINUTE_MILLISECOND || differenceMillisecond > THIRTY_DAYS_MILLISECOND) {
                throw new KylinException(TIME_INVALID_RANGE_IN_RANGE);
            }
        }
    }

    public void validateDateTimeFormatPattern(String pattern) {
        if (pattern.isEmpty()) {
            throw new KylinException(DATETIME_FORMAT_EMPTY);
        }
        try {
            new SimpleDateFormat(pattern, Locale.getDefault(Locale.Category.FORMAT));
        } catch (IllegalArgumentException e) {
            throw new KylinException(DATETIME_FORMAT_PARSE_ERROR, e, pattern);
        }
    }

    public void checkStreamingOperation(String project, String table) {
        val config = KylinConfig.getInstanceFromEnv();
        val kafkaConf = KafkaConfigManager.getInstance(config, project).getKafkaConfig(table);
        if (kafkaConf != null) {
            throw new KylinException(UNSUPPORTED_STREAMING_OPERATION,
                    MsgPicker.getMsg().getStreamingOperationNotSupport());
        }
    }

    private ResponseEntity<byte[]> getHttpResponse(final HttpServletRequest request, String url) throws IOException {
        byte[] body = IOUtils.toByteArray(request.getInputStream());
        HttpHeaders headers = new HttpHeaders();
        Collections.list(request.getHeaderNames())
                .forEach(k -> headers.put(k, Collections.list(request.getHeaders(k))));
        //remove gzip
        headers.remove(ACCEPT_ENCODING);
        return restTemplate.exchange(url, HttpMethod.valueOf(request.getMethod()), new HttpEntity<>(body, headers),
                byte[].class);
    }

    public <T> EnvelopeResponse<T> generateTaskForRemoteHost(final HttpServletRequest request, String url)
            throws Exception {
        val response = getHttpResponse(request, url);
        return JsonUtil.readValue(response.getBody(), EnvelopeResponse.class);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void downloadFromRemoteHost(final HttpServletRequest request, String url,
            HttpServletResponse servletResponse) throws IOException {
        File temporaryZipFile = KylinConfigBase.getDiagFileName();
        temporaryZipFile.getParentFile().mkdirs();
        Preconditions.checkState(temporaryZipFile.createNewFile(), "create temporary zip file failed");
        RequestCallback requestCallback = x -> {
            Collections.list(request.getHeaderNames())
                    .forEach(k -> x.getHeaders().put(k, Collections.list(request.getHeaders(k))));
            x.getHeaders().setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM, MediaType.ALL));
            x.getHeaders().remove(ACCEPT_ENCODING);
        };
        ResponseExtractor<String> responseExtractor = x -> {
            try (FileOutputStream fout = new FileOutputStream(temporaryZipFile)) {
                StreamUtils.copy(x.getBody(), fout);
            }
            val name = x.getHeaders().get(CONTENT_DISPOSITION);
            return name == null ? "error" : name.get(0);
        };

        String fileName = restTemplate.execute(url, HttpMethod.GET, requestCallback, responseExtractor);

        try (InputStream in = Files.newInputStream(temporaryZipFile.toPath());
                OutputStream out = servletResponse.getOutputStream()) {
            servletResponse.reset();
            servletResponse.setContentLengthLong(temporaryZipFile.length());
            servletResponse.setContentType(MediaType.APPLICATION_OCTET_STREAM.toString());
            servletResponse.setHeader(CONTENT_DISPOSITION, fileName);
            IOUtils.copy(in, out);
            out.flush();
        } finally {
            FileUtils.deleteQuietly(temporaryZipFile);
        }
    }

    public void checkStreamingEnabled() {
        if (!KylinConfig.getInstanceFromEnv().streamingEnabled()) {
            throw new KylinException(ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION,
                    MsgPicker.getMsg().getStreamingDisabled());
        }
    }

    public String getInsensitiveProject(String project) {
        if (StringUtils.isEmpty(project)) {
            return "";
        }
        return getProjectNameFromEnv(project);
    }

    public String getProjectNameFromEnv(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        return projectInstance.getName();
    }

    public String getInsensitiveProjectModelName(String project, String modelAlias) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel dataModel = modelManager.getDataModelDescByAlias(modelAlias);
        return dataModel != null ? dataModel.getAlias() : modelAlias;
    }

    public void checkStreamingJobsStatus(List<String> statuses) {
        if (CollectionUtils.isEmpty(statuses)) {
            return;
        }
        List<String> streamingJobsStatus = Arrays.asList(JobStatusEnum.STARTING.name(), JobStatusEnum.RUNNING.name(),
                JobStatusEnum.STOPPING.name(), JobStatusEnum.ERROR.name(), JobStatusEnum.STOPPED.name());
        for (String status : statuses) {
            if (!streamingJobsStatus.contains(status)) {
                throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "statuses",
                        org.apache.commons.lang.StringUtils.join(streamingJobsStatus, ", "));
            }
        }
    }

    public void checkStreamingJobTypeStatus(List<String> statuses) {
        if (CollectionUtils.isEmpty(statuses)) {
            return;
        }
        List<String> streamingJobTypeStatus = Arrays.asList(JobTypeEnum.STREAMING_BUILD.name(),
                JobTypeEnum.STREAMING_MERGE.name());
        for (String status : statuses) {
            if (!streamingJobTypeStatus.contains(status)) {
                throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "job_types",
                        org.apache.commons.lang.StringUtils.join(streamingJobTypeStatus, ", "));
            }
        }
    }

    protected void checkCollectionRequiredArg(String fieldName, Collection<?> fieldValue) {
        if (CollectionUtils.isEmpty(fieldValue)) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, fieldName);
        }
    }

    public String decodeHost(String host) {
        try {
            if (StringUtils.isBlank(host) || host.startsWith("http://")) {
                return host;
            }
            String decryptValue = EncryptUtil.decrypt(new String(Base64.decodeBase64(host), Charset.defaultCharset()));
            return StringUtils.isBlank(decryptValue) ? host : decryptValue;
        } catch (Exception e) {
            logger.error("Failed to decode host, will use the original host name");
        }
        return host;
    }

    public String encodeHost(String host) {
        try {
            if (StringUtils.isBlank(host)) {
                return host;
            }
            host = host.trim();
            if (!host.toLowerCase().startsWith("http")) {
                host = "http://" + host;
            }
            return Base64.encodeBase64String(EncryptUtil.encrypt(host).getBytes(Charset.defaultCharset()));
        } catch (Exception e) {
            logger.error("Failed to encode host, will use the original host name");
        }
        return host;
    }
}
