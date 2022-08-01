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

import static io.kyligence.kap.guava20.shaded.common.net.HttpHeaders.CONTENT_DISPOSITION;
import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CONNECT_CATALOG;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DOWNLOAD_FILE;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_UNAUTHORIZED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.BOOLEAN_TYPE_CHECK;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.INTEGER_NON_NEGATIVE_CHECK;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_CONFLICT_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_LESS_THAN_ZERO;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_CONSISTENT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_FORMAT_MS;
import static org.apache.kylin.metadata.model.PartitionDesc.transformTimestamp2Format;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.rest.request.Validation;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import com.google.common.collect.Lists;

import lombok.SneakyThrows;
import lombok.val;

public class BaseController {
    private static final Logger logger = LoggerFactory.getLogger(BaseController.class);
    protected static final int MAX_NAME_LENGTH = 50;

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
        return new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(req), cause);
    }

    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ExceptionHandler(ForbiddenException.class)
    @ResponseBody
    ErrorResponse handleForbidden(HttpServletRequest req, Exception ex) {
        getLogger().error("", ex);
        return new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(req), ex);
    }

    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(NotFoundException.class)
    @ResponseBody
    ErrorResponse handleNotFound(HttpServletRequest req, Exception ex) {
        getLogger().error("", ex);
        return new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(req), ex);
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
        return new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(req), e);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({ MethodArgumentTypeMismatchException.class, MissingServletRequestParameterException.class,
            IllegalArgumentException.class })
    @ResponseBody
    ErrorResponse handleInvalidRequestParam(HttpServletRequest req, Throwable ex) {
        KylinException e = new KylinException(INVALID_PARAMETER, ex);
        getLogger().error("", e);
        return new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(req), e);
    }

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(KylinException.class)
    @ResponseBody
    ErrorResponse handleErrorCode(HttpServletRequest req, Throwable ex) {
        getLogger().error("", ex);
        KylinException cause = (KylinException) ex;
        return new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(req), cause);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    ErrorResponse handleInvalidArgument(HttpServletRequest request, MethodArgumentNotValidException ex) {
        val response = new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(request), ex);
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
        return new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(req), ex);
    }

    protected void checkRequiredArg(String fieldName, Object fieldValue) {
        if (fieldValue == null || StringUtils.isEmpty(String.valueOf(fieldValue))) {
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
            throw new KylinException(BOOLEAN_TYPE_CHECK, booleanString, "Boolean");
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
            }
        }
    }

    public HashMap<String, Object> getDataResponse(String name, List<?> result, int offset, int limit) {
        HashMap<String, Object> data = new HashMap<>();
        data.put(name, PagingUtil.cutPage(result, offset, limit));
        data.put("size", result.size());
        return data;
    }

    public String checkProjectName(String project) {
        if (StringUtils.isEmpty(project)) {
            throw new KylinException(EMPTY_PROJECT_NAME, MsgPicker.getMsg().getEmptyProjectName());
        }

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prjInstance = projectManager.getProject(project);
        if (prjInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        return prjInstance.getName();
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

    protected void checkSegmentParams(String[] ids, String[] names) {

        //both not empty
        if (ArrayUtils.isNotEmpty(ids) && ArrayUtils.isNotEmpty(names)) {
            throw new KylinException(SEGMENT_CONFLICT_PARAMETER);
        }

        //both empty
        if (ArrayUtils.isEmpty(ids) && ArrayUtils.isEmpty(names)) {
            throw new KylinException(SEGMENT_EMPTY_PARAMETER);
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
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "status",
                    "ONLINE, OFFLINE, WARNING, BROKEN");
        }
        return formattedStatus;
    }

    public void validatePriority(int priority) {
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
        validateDataRange(start, end, null);
    }

    public void validateDataRange(String start, String end, String partitionColumnFormat) {
        if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end)) {
            return;
        }

        if (StringUtils.isNotEmpty(start) && StringUtils.isNotEmpty(end)) {
            long startLong = 0;
            long endLong = 0;

            try {
                startLong = Long.parseLong(start);
                endLong = Long.parseLong(end);
            } catch (Exception e) {
                throw new KylinException(TIME_INVALID_RANGE_NOT_FORMAT_MS);
            }

            if (startLong < 0 || endLong < 0)
                throw new KylinException(TIME_INVALID_RANGE_LESS_THAN_ZERO);

            try {
                startLong = DateFormat.getFormatTimeStamp(start, transformTimestamp2Format(partitionColumnFormat));
                endLong = DateFormat.getFormatTimeStamp(end, transformTimestamp2Format(partitionColumnFormat));
            } catch (Exception e) {
                throw new KylinException(TIME_INVALID_RANGE_NOT_FORMAT_MS);
            }

            if (startLong >= endLong)
                throw new KylinException(TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START);

        } else {
            throw new KylinException(TIME_INVALID_RANGE_NOT_CONSISTENT);
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

}
