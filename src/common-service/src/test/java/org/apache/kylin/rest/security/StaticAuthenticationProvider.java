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

package org.apache.kylin.rest.security;

import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.security.core.Authentication;

public class StaticAuthenticationProvider extends OpenAuthenticationProvider {
    @Override
    public boolean authenticateImpl(Authentication authentication) {
        String name = authentication.getName();
        Object credentials = authentication.getCredentials();
        ManagedUser user = (ManagedUser) getUserService().loadUserByUsername(name);
        if (!credentials.equals(user.getPassword())) {
            return false;
        }
        return true;
    }
}
