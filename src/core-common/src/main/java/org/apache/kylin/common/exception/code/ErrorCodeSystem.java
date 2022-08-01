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
package org.apache.kylin.common.exception.code;

public enum ErrorCodeSystem implements ErrorCodeProducer {

    // 400052XX password
    PASSWORD_INVALID_ENCODER("KE-040005201"), PASSWORD_INIT_ENCODER_FAILED("KE-040005202"),

    // 400212XX epoch
    EPOCH_DOES_NOT_BELONG_TO_CURRENT_NODE("KE-040021201"),

    // 400232XX HA
    JOB_NODE_API_INVALID("KE-040023201"), JOB_NODE_QUERY_API_INVALID("KE-040023202"), QUERY_NODE_API_INVALID(
            "KE-040023203"),

    // 400242XX maintenance mode
    MAINTENANCE_MODE_WRITE_FAILED("KE-040024201"), MAINTENANCE_MODE_ENTER_FAILED(
            "KE-040024202"), MAINTENANCE_MODE_LEAVE_FAILED("KE-040024203"),

    // 400252XX system config
    SYSTEM_PROFILE_ABNORMAL_DATA("KE-040026201");

    private final ErrorCode errorCode;
    private final ErrorMsg errorMsg;
    private final ErrorSuggestion errorSuggestion;

    ErrorCodeSystem(String keCode) {
        this.errorCode = new ErrorCode(keCode);
        this.errorMsg = new ErrorMsg(this.errorCode.getCode());
        this.errorSuggestion = new ErrorSuggestion(this.errorCode.getCode());
    }

    @Override
    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    @Override
    public ErrorMsg getErrorMsg() {
        return this.errorMsg;
    }

    @Override
    public ErrorSuggestion getErrorSuggest() {
        return this.errorSuggestion;
    }
}
