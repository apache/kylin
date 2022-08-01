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

package org.apache.kylin.rest.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SystemProfileResponse {

    @JsonProperty("isCloud")
    private Boolean isCloud = true;
    @JsonProperty("securityProfile")
    private String securityProfile;
    @JsonProperty("authType")
    private String authType;
    @JsonProperty("ldapServer")
    private String ldapServer;
    @JsonProperty("jdbcType")
    private String jdbcType;

    @JsonUnwrapped
    @JsonProperty("aadProperties")
    private AADProperties aadProperties;

    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AADProperties {
        @JsonProperty("clientId")
        private String clientId;
        @JsonProperty("tenantId")
        private String tenantId;
        @JsonProperty("clientSecret")
        private String clientSecret;
    }
}
