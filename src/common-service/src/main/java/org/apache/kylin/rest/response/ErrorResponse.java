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

package org.apache.kylin.rest.response;

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_PARSE_JSON;
import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * response to client when the return HTTP code is not 200
 */
@Slf4j
@Data
@NoArgsConstructor
public class ErrorResponse extends EnvelopeResponse<Object> {

    //stacktrace of the exception
    private String stacktrace;

    //same as EnvelopeResponse.msg, kept for legacy reasons
    private String exception;

    //request URL, kept from legacy codes
    private String url;

    private String suggestion;

    @JsonProperty("error_code")
    public String errorCode;

    public static final String STACK_MSG = "Kylin Service Intercept Stack Feature Enabled.";

    public ErrorResponse(String url, Throwable exception) {
        super();
        this.url = url;
        this.exception = exception.getLocalizedMessage();
        this.data = null;

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String stackTraceAsString = Throwables.getStackTraceAsString(exception);
        if (exception instanceof KylinException) {
            this.msg = exception.getLocalizedMessage();
            KylinException kylinException = (KylinException) exception;
            this.code = kylinException.getCode();
            this.suggestion = kylinException.getSuggestionString();
            this.errorCode = kylinException.getErrorCodeString();
            if (kylinException.isThrowTrace()) {
                if (config.isGlobalStackInterceptionEnabled()) {
                    this.stacktrace = STACK_MSG;
                    log.error(stackTraceAsString);
                } else {
                    this.stacktrace = stackTraceAsString;
                }
            }
            this.data = kylinException.getData();
        } else {
            String errorCodeString = UNKNOWN_ERROR_CODE.toErrorCode().getLocalizedString();
            if (exception.getClass() == JsonParseException.class) {
                errorCodeString = FAILED_PARSE_JSON.toErrorCode().getLocalizedString();
            }

            this.msg = errorCodeString + " " + exception.getLocalizedMessage();
            this.code = KylinException.CODE_UNDEFINED;
            if (config.isGlobalStackInterceptionEnabled()) {
                this.stacktrace = STACK_MSG;
                log.error(stackTraceAsString);
            } else {
                this.stacktrace = Throwables.getStackTraceAsString(exception);
            }
        }
    }

}
