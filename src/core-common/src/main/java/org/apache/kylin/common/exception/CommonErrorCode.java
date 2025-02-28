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

public enum CommonErrorCode implements ErrorCodeSupplier {

    // 00000XXX general
    UNKNOWN_ERROR_CODE("KE-000000001"), //

    // 00000XXX general
    TIMEOUT("KE-000000002"), //

    // 00006XXX column
    INVALID_TIME_PARTITION_COLUMN("KE-000006001"), //

    // 00025XXX shell
    FAILED_EXECUTE_SHELL("KE-000025001"), //
    FAILED_PARSE_SHELL("KE-000025002"), //

    // 00027XXX metadata
    FAILED_UPDATE_METADATA("KE-000027001"), //
    FAILED_NOTIFY_CATCHUP("KE-000027002"), //
    FAILED_CONNECT_META_DATABASE("KE-000027003"), //
    FAILED_FORWARD_METADATA_ACTION("KE-000027004"), //
    INTERNAL_TABLE_UNSUPPORTED_STORAGE_TYPE("KE-000027005"), //
    INTERNAL_TABLE_INVALID_BUCKET_NUMBER("KE-000027006"), //

    // 00028XXX source usage
    LICENSE_OVER_CAPACITY("KE-000028001"), //

    // 00029XXX file
    INVALID_ZIP_NAME("KE-000029001"), //
    INVALID_ZIP_ENTRY("KE-000029002"), //

    // 00030XXX json
    FAILED_PARSE_JSON("KE-000030001");

    private final ErrorCode errorCode;

    CommonErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
