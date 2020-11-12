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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.TooManyRequestException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import static org.apache.kylin.shaded.com.google.common.net.HttpHeaders.CONTENT_DISPOSITION;

/**
 */
public class BasicController {

    private static final Logger logger = LoggerFactory.getLogger(BasicController.class);

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(Exception.class)
    @ResponseBody
    ErrorResponse handleError(HttpServletRequest req, Exception ex) {
        logger.error("", ex);

        Message msg = MsgPicker.getMsg();
        Throwable cause = ex;
        while (cause != null) {
            if (cause.getClass().getPackage().getName().startsWith("org.apache.hadoop.hbase")) {
                return new ErrorResponse(req.getRequestURL().toString(), new InternalErrorException(
                        String.format(Locale.ROOT, msg.getHBASE_FAIL(), ex.getMessage()), ex));
            }
            cause = cause.getCause();
        }

        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ExceptionHandler(ForbiddenException.class)
    @ResponseBody
    ErrorResponse handleForbidden(HttpServletRequest req, Exception ex) {
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(NotFoundException.class)
    @ResponseBody
    ErrorResponse handleNotFound(HttpServletRequest req, Exception ex) {
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(BadRequestException.class)
    @ResponseBody
    ErrorResponse handleBadRequest(HttpServletRequest req, Exception ex) {
        logger.error("", ex);
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(UnauthorizedException.class)
    @ResponseBody
    ErrorResponse handleUnauthorized(HttpServletRequest req, Exception ex) {
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    @ExceptionHandler(TooManyRequestException.class)
    @ResponseBody
    ErrorResponse handleTooManyRequest(HttpServletRequest req, Exception ex) {
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    protected void checkRequiredArg(String fieldName, Object fieldValue) {
        if (fieldValue == null || StringUtils.isEmpty(String.valueOf(fieldValue))) {
            throw new BadRequestException(fieldName + " is required");
        }
    }

    protected void setDownloadResponse(String downloadFile, final HttpServletResponse response) {
        File file = new File(downloadFile);
        try (InputStream fileInputStream = new FileInputStream(file);
                OutputStream output = response.getOutputStream()) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.setContentLength((int) (file.length()));
            response.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"");
            IOUtils.copyLarge(fileInputStream, output);
            output.flush();
        } catch (IOException e) {
            throw new InternalErrorException("Failed to download file: " + e.getMessage(), e);
        }
    }

    protected void setDownloadResponse(InputStream inputStream, String fileName, String contentType,
                                       final HttpServletResponse response) throws IOException {
        try (OutputStream output = response.getOutputStream()) {
            response.reset();
            response.setContentType(contentType);
            response.setHeader(CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            IOUtils.copyLarge(inputStream, output);
            output.flush();
        } catch (IOException e) {
            logger.error("Failed download log File!");
            throw e;
        }
    }

    public boolean isAdmin() {
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

}
