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
package org.apache.kylin.common.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import lombok.Getter;

public class ClusterConstant implements Serializable {
    private ClusterConstant() {
    }

    public static final String QUERY = ServerModeEnum.QUERY.name;
    public static final String ALL = ServerModeEnum.ALL.name;
    public static final String JOB = ServerModeEnum.JOB.name;
    public static final String DATA_LOADING = ServerModeEnum.DATA_LOADING.name;
    public static final String SMART = ServerModeEnum.SMART.name;
    public static final String COMMON = ServerModeEnum.COMMON.name;
    public static final String RESOURCE = ServerModeEnum.RESOURCE.name;
    public static final String OPS = ServerModeEnum.OPS.name;
    public static final List<String> ALL_MICRO_TYPE = Arrays.asList(COMMON, QUERY, SMART, DATA_LOADING, OPS, RESOURCE);

    @Getter
    public enum ServerModeEnum {
        QUERY("query"), ALL("all"), JOB("job"), DATA_LOADING("data-loading"), SMART("smart"), COMMON(
                "common"), RESOURCE("resource"), OPS("ops");

        private final String name;

        ServerModeEnum(String name) {
            this.name = name;
        }

        public static ServerModeEnum of(String name) {
            for (ServerModeEnum e : values()) {
                if (e.name.equalsIgnoreCase(name)) {
                    return e;
                }
            }
            return null;
        }
    }
}
