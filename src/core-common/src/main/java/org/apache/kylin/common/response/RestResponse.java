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

package org.apache.kylin.common.response;

import org.apache.kylin.common.exception.KylinException;

public class RestResponse<T> {

    protected String code;
    protected T data;
    protected String msg;

    //only for child
    protected RestResponse() {
    }

    public static RestResponse ok(Object data) {
        return new RestResponse(KylinException.CODE_SUCCESS, data, "");
    }

    public static RestResponse ok() {
        return new RestResponse(KylinException.CODE_SUCCESS, "", "");
    }

    public static RestResponse fail() {
        return new RestResponse("200", "", "");
    }

    public RestResponse(T data) {
        this(KylinException.CODE_SUCCESS, data, "");
    }

    public RestResponse(String code, T data, String msg) {
        this.code = code;
        this.data = data;
        this.msg = msg;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
