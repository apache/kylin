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

import java.util.Locale;

import lombok.Getter;

@Getter
public enum ErrorCodeCommon implements ErrorCodeProducer {

    NON_KE_EXCEPTION("KE-060100201");

    private final ErrorCode errorCode;
    private final ErrorMsg errorMsg;
    private final ErrorSuggestion errorSuggestion;

    ErrorCodeCommon(String keCode) {
        this.errorCode = new ErrorCode(keCode);
        this.errorMsg = new ErrorMsg(this.errorCode.getCode());
        this.errorSuggestion = new ErrorSuggestion(this.errorCode.getCode());
    }

    @Override
    public ErrorSuggestion getErrorSuggest() {
        return this.errorSuggestion;
    }

    @Override
    public String getMsg(Object... args) {
        return String.format(Locale.ROOT, this.errorMsg.getLocalizedString(), args);
    }

    @Override
    public String getCodeMsg(Object... args) {
        return String.format(Locale.ROOT, CODE_MSG_FORMAT, errorCode.getCode(), getMsg(args));
    }
}
