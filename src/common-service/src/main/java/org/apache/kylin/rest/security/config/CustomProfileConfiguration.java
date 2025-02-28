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

package org.apache.kylin.rest.security.config;

import java.lang.reflect.InvocationTargetException;

import org.apache.kylin.rest.service.OpenUserGroupService;
import org.apache.kylin.rest.service.OpenUserService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AuthenticationProvider;

@Profile("custom")
@Configuration
public class CustomProfileConfiguration {

    @Value("${kylin.security.custom.user-service-class-name}")
    private String userClassName;

    @Value("${kylin.security.custom.user-group-service-class-name}")
    private String userGroupClassName;

    @Value("${kylin.security.custom.authentication-provider-class-name}")
    private String authenticationProviderClassName;

    @Bean("userService")
    public OpenUserService userService() throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            NoSuchMethodException, InvocationTargetException {
        Class<?> serviceClass = Class.forName(userClassName);
        return (OpenUserService) serviceClass.getDeclaredConstructor().newInstance();
    }

    @Bean("userGroupService")
    public OpenUserGroupService userGroupService() throws ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, NoSuchMethodException {
        Class<?> serviceClass = Class.forName(userGroupClassName);
        return (OpenUserGroupService) serviceClass.getDeclaredConstructor().newInstance();
    }

    @Bean("customAuthProvider")
    public AuthenticationProvider customAuthProvider() throws ClassNotFoundException, InvocationTargetException,
            InstantiationException, IllegalAccessException, NoSuchMethodException {
        Class<?> serviceClass = Class.forName(authenticationProviderClassName);
        return (AuthenticationProvider) serviceClass.getDeclaredConstructor().newInstance();
    }
}
