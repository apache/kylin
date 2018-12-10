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

package org.apache.kylin.stream.server.rest.controller;

import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.stream.server.rest.exception.BadRequestException;
import org.apache.kylin.stream.server.rest.exception.ForbiddenException;
import org.apache.kylin.stream.server.rest.exception.NotFoundException;
import org.apache.kylin.stream.server.rest.model.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 */
public class BasicController {

    private static final Logger logger = LoggerFactory.getLogger(BasicController.class);

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(Exception.class)
    @ResponseBody
    ErrorResponse handleError(HttpServletRequest req, Exception ex) {
        logger.error("", ex);
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
}
