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

public enum ToolErrorCode implements ErrorCodeSupplier {
    // 50001XXX project
    EMPTY_PROJECT_PARAMETER("KE-050001001"), //
    PROJECT_PARAMETER_NOT_EXIST("KE-050001002"), //

    // 50007XXX table
    EMPTY_TABLE_PARAMETER("KE-050007001"), //
    TABLE_PARAMETER_NOT_EXIST("KE-050007002"), //

    // 50013XXX job
    EMPTY_JOB_PARAMETER("KE-050013001"), //

    // 50017XXX file
    DIRECTORY_NOT_EXIST("KE-050017001"), //
    FILE_ALREADY_EXIST("KE-050017002"), //

    // 50023XXX diag
    DIAG_STARTTIME_PARAMETER_NOT_EXIST("KE-050023001"), //
    DIAG_ENDTIME_PARAMETER_NOT_EXIST("KE-050023002"), //
    DIAG_TIMEOUT("KE-050023003"), //

    // 50025XXX shell
    INVALID_SHELL_PARAMETER("KE-050025001");//

    private final ErrorCode errorCode;

    ToolErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
