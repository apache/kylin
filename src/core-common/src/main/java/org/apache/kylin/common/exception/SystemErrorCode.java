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

public enum SystemErrorCode implements ErrorCodeSupplier {
    // 40005XXX password
    INVALID_PASSWORD_ENCODER("KE-040005001"), //
    FAILED_INIT_PASSWORD_ENCODER("KE-040005002"), //

    // 40021XXX server
    EPOCH_DOES_NOT_BELONG_TO_CURRENT_NODE("KE-040021001"), //

    // 40022XXX segment
    FAILED_MERGE_SEGMENT("KE-040022001"), //

    // 40023XXX job
    JOBNODE_API_INVALID("KE-040023002"), //
    QUERYNODE_API_INVALID("KE-040023003"), //

    // 40024XXX read mode
    WRITE_IN_MAINTENANCE_MODE("KE-040024001"), //
    FAILED_ENTER_MAINTENANCE_MODE("KE-040024002"), //
    FAILED_LEAVE_MAINTENANCE_MODE("KE-040024003");

    private final ErrorCode errorCode;

    SystemErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
