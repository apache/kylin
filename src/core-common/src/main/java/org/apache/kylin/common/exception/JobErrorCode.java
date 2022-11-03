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

public enum JobErrorCode implements ErrorCodeSupplier {

    JOB_BUILDING_ERROR("KE-030001000"),
    SANITY_CHECK_ERROR("KE-030001001"),
    JOB_CONFIGURATION_ERROR("KE-030001002"),
    JOB_DATE_FORMAT_NOT_MATCH_ERROR("KE-030001003"),
    JOB_OUT_OF_MEMORY_ERROR("KE-030001004"),
    JOB_NO_SPACE_LEFT_ON_DEVICE_ERROR("KE-030001005"),
    JOB_CLASS_NOT_FOUND_ERROR("KE-030001006"),
    KERBEROS_REALM_NOT_FOUND("KE-030001007"),
    JOB_INT_DATE_FORMAT_NOT_MATCH_ERROR("KE-030001008"),
    UNABLE_CONNECT_SPARK_MASTER_MAXIMUM_TIME("KE-030001009"),
    PROFILING_NOT_ENABLED("KE-030001010"),
    PROFILING_STATUS_ERROR("KE-030001011"),
    PROFILING_COLLECT_TIMEOUT("KE-030001012"),

    SECOND_STORAGE_SEGMENTS_CONFLICTS("KE-030002001"),
    SECOND_STORAGE_JOB_EXISTS("KE-030002002"),
    SECOND_STORAGE_PROJECT_JOB_EXISTS("KE-030002003");

    private final ErrorCode errorCode;

    JobErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
