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
package org.apache.kylin.rest.advice;

import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@ControllerAdvice
public class BaseExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(BaseExceptionHandler.class);

    @ExceptionHandler(ForbiddenException.class)
    public final ResponseEntity<Void> handle(ForbiddenException ex, WebRequest request) {
        logger.error("uncaught exception", ex);
        return new ResponseEntity<>(HttpStatus.FORBIDDEN);
    }

    @ExceptionHandler(InternalErrorException.class)
    public final ResponseEntity<Void> handle(InternalErrorException ex, WebRequest request) {
        logger.error("uncaught exception", ex);
        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(NotFoundException.class)
    public final ResponseEntity<Void> handle(NotFoundException ex, WebRequest request) {
        logger.error("uncaught exception", ex);
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(UnauthorizedException.class)
    public final ResponseEntity<Void> handle(UnauthorizedException ex, WebRequest request) {
        logger.error("uncaught exception", ex);
        return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
    }
}
