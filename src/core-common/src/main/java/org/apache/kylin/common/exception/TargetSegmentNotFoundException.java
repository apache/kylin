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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.msg.MsgPicker;

import static org.apache.kylin.common.exception.QueryErrorCode.TARGET_SEGMENT_NOT_FOUND;

public class TargetSegmentNotFoundException extends KylinException {

    public TargetSegmentNotFoundException(String message) {
        super(TARGET_SEGMENT_NOT_FOUND, MsgPicker.getMsg().getTargetSegmentNotFoundError(message));
    }

    public static boolean causedBySegmentNotFound(Throwable e) {
        return e instanceof TargetSegmentNotFoundException
                || ExceptionUtils.getRootCause(e) instanceof TargetSegmentNotFoundException;
    }

}
