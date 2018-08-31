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

package org.apache.kylin.common.persistence;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BrokenEntity extends RootPersistentEntity {

    public static final byte[] MAGIC = new byte[]{'B', 'R', 'O', 'K', 'E', 'N'};

    @JsonProperty("resPath")
    private String resPath;

    @JsonProperty("errorMsg")
    private String errorMsg;

    public BrokenEntity() {
    }

    public BrokenEntity(String resPath, String errorMsg) {
        this.resPath = resPath;
        this.errorMsg = errorMsg;
    }

    public String getResPath() {
        return resPath;
    }

    public void setResPath(String resPath) {
        this.resPath = resPath;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
