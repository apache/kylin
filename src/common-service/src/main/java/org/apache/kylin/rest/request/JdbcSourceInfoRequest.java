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

package org.apache.kylin.rest.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JdbcSourceInfoRequest {
    @JsonProperty("jdbc_source_name")
    private String jdbcSourceName;
    @JsonProperty("jdbc_source_user")
    private String jdbcSourceUser;
    @JsonProperty("jdbc_source_pass")
    private String jdbcSourcePass;
    @JsonProperty("jdbc_source_connection_url")
    private String jdbcSourceConnectionUrl;
    @JsonProperty("jdbc_source_enable")
    private Boolean jdbcSourceEnable;
    @JsonProperty("jdbc_source_driver")
    private String jdbcSourceDriver;

}
