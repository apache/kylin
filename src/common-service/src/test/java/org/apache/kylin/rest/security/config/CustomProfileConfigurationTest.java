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

import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.security.StaticAuthenticationProvider;
import org.apache.kylin.rest.service.OpenUserGroupService;
import org.apache.kylin.rest.service.OpenUserService;
import org.apache.kylin.rest.service.StaticUserGroupService;
import org.apache.kylin.rest.service.StaticUserService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.web.client.RestTemplate;

class CustomProfileConfigurationTest {

    private SpringApplication application;

    @BeforeEach
    void setup() {
        this.application = new SpringApplication(Config.class, CustomProfileConfiguration.class);
        this.application.setWebApplicationType(WebApplicationType.NONE);
    }

    @Test
    void testCustomProfile() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all",
                "--spring.main.allow-circular-references=true", "--spring.profiles.active=custom",
                "--kylin.security.custom.user-service-class-name=org.apache.kylin.rest.service.StaticUserService",
                "--kylin.security.custom.user-group-service-class-name=org.apache.kylin.rest.service.StaticUserGroupService",
                "--kylin.security.custom.authentication-provider-class-name=org.apache.kylin.rest.security.StaticAuthenticationProvider")) {
            OpenUserService userService = context.getBean("userService", OpenUserService.class);
            Assertions.assertTrue(userService instanceof StaticUserService);
            OpenUserGroupService userGroupService = context.getBean("userGroupService", OpenUserGroupService.class);
            Assertions.assertTrue(userGroupService instanceof StaticUserGroupService);
            AuthenticationProvider authenticationProvider = context.getBean("customAuthProvider",
                    AuthenticationProvider.class);
            Assertions.assertTrue(authenticationProvider instanceof StaticAuthenticationProvider);
        }

    }

    @Configuration
    @Profile("custom")
    static class Config {

        @Bean("normalRestTemplate")
        public RestTemplate restTemplate() {
            return new RestTemplate();
        }

        @Bean
        public AclService aclService() {
            return new AclService();
        }

        @Bean
        public AccessService accessService() {
            return new AccessService();
        }

        @Bean
        public AclUtil aclUtil() {
            return new AclUtil();
        }

        @Bean
        public AclEvaluate aclEvaluate() {
            return new AclEvaluate();
        }
    }

}
