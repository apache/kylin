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

public enum JobExceptionReason implements ExceptionReasonSupplier {
    JOB_BUILDING_ERROR("KE-030001000"),
    JOB_DATE_FORMAT_NOT_MATCH_ERROR("KE-030001003"),
    JOB_OUT_OF_MEMORY_ERROR("KE-030001004"),
    JOB_NO_SPACE_LEFT_ON_DEVICE_ERROR("KE-030001005"),
    JOB_CLASS_NOT_FOUND_ERROR("KE-030001006"),
    KERBEROS_REALM_NOT_FOUND("KE-030001007"),
    JOB_INT_DATE_FORMAT_NOT_MATCH_ERROR("KE-030001008"),
    UNABLE_CONNECT_SPARK_MASTER_MAXIMUM_TIME("KE-030001009");

    private final ExceptionReason reason;

    JobExceptionReason(String code) {
        reason = new ExceptionReason(code);
    }

    @Override
    public ExceptionReason toExceptionReason() {
        return reason;
    }
}
