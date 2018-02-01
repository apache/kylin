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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.rest.security.ManagedUser;
import org.springframework.security.provisioning.UserDetailsManager;

public interface UserService extends UserDetailsManager {

    boolean isEvictCacheFlag();

    void setEvictCacheFlag(boolean evictCacheFlag);

    List<ManagedUser> listUsers() throws IOException;

    List<String> listUsernames() throws IOException;

    List<String> listAdminUsers() throws IOException;

    //For performance consideration, list all users may be incomplete(eg. not load user's authorities until authorities has benn used).
    //So it's an extension point that can complete user's information latter.
    //loadUserByUsername() has guarantee that the return user is complete.
    void completeUserInfo(ManagedUser user);
}
