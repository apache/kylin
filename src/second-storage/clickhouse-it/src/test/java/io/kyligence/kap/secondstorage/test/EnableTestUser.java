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

package io.kyligence.kap.secondstorage.test;

import org.apache.kylin.rest.constant.Constant;
import org.junit.rules.ExternalResource;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public class EnableTestUser extends ExternalResource {
    public static final String ADMIN = "ADMIN";
    private final Authentication authentication = new TestingAuthenticationToken(ADMIN, ADMIN, Constant.ROLE_ADMIN);

    @Override
    protected void before() throws Throwable {
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    public String getUser() {
        return ADMIN;
    }

    @Override
    protected void after() {
        SecurityContextHolder.getContext().setAuthentication(null);
    }
}
