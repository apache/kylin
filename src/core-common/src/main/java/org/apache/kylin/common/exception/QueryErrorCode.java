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
 
package org.apache.kylin.common.exception;

public enum QueryErrorCode implements ErrorCodeSupplier {

    // 20002XXX model
    CC_EXPRESSION_ILLEGAL("KE-020002007"),

    // 20003XXX user
    USER_STOP_QUERY("KE-020003001"), //

    // 20007XXX table
    EMPTY_TABLE("KE-020007001"), //

    // 20008XXX general query errors
    UNSUPPORTED_EXPRESSION("KE-020008001"),
    UNSUPPORTED_OPERATION("KE-020008002"),
    TARGET_SEGMENT_NOT_FOUND("KE-020008003"),

    // 20029XXX optimization rule
    UNSUPPORTED_SUM_CASE_WHEN("KE-020029001"), //

    // 20030XXX push down
    INVALID_PARAMETER_PUSH_DOWN("KE-020030001"), //
    NO_AUTHORIZED_COLUMNS("KE-020030002"), //

    // 20032XXX query busy
    BUSY_QUERY("KE-020032001"), //

    // 20040XXX async query
    ASYNC_QUERY_ILLEGAL_PARAM("KE-020040001"),
    TOO_MANY_ASYNC_QUERY("KE-020040002"),

    // 20050XXX invalid query params
    INVALID_QUERY_PARAMS("KE-020050001"),

    // 20060XXX parse error
    FAILED_PARSE_ERROR("KE-020060001"),

    // 20070XXX parse error
    PROFILING_NOT_ENABLED("KE-020070001"),
    PROFILING_ALREADY_STARTED("KE-020070002"),
    PROFILER_ALREADY_DUMPED("KE-020070003"),

    // 20080XXX query limit
    REFUSE_NEW_QUERY("KE-020080001"),
    BIG_QUERY_DECIDE("KE-020008002");

    private final ErrorCode errorCode;

    QueryErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
