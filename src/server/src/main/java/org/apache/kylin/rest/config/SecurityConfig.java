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

package org.apache.kylin.rest.config;

import javax.servlet.Filter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Order(200)
@Configuration
@EnableWebSecurity
@Profile({ "testing", "ldap", "custom" })
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    @Autowired
    Filter fillEmptyAuthorizationFilter;

    @Autowired
    LogoutSuccessHandler logoutSuccessHandler;

    @Autowired
    AuthenticationEntryPoint nUnauthorisedEntryPoint;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        // @formatter:off
        // https://docs.spring.io/spring-security/site/docs/3.2.x/reference/htmlsingle/html5/#nsa-http-attributes
        http.formLogin()
                .and()
                .httpBasic()
                .authenticationEntryPoint(nUnauthorisedEntryPoint)
                .and()
                .logout();

        // https://spring.io/blog/2013/07/11/spring-security-java-config-preview-readability/
        // use-expressions="true"

        http.headers()
                .frameOptions(HeadersConfigurer.FrameOptionsConfig::sameOrigin)
                .and()
                .csrf(AbstractHttpConfigurer::disable)
                .addFilterBefore(fillEmptyAuthorizationFilter, BasicAuthenticationFilter.class);

        http.authorizeRequests()
                .antMatchers("/api/streaming_jobs/spark", "/api/streaming_jobs/stats",
                        "/api/streaming_jobs/dataflow/**", "/api/epoch/maintenance_mode", "/api/health", "/api/health/**",
                        "/api/prometheus", "/api/monitor/spark/prometheus", "/api/user/update_user", "/api/metastore/cleanup",
                        "/api/metastore/cleanup_storage", "/api/epoch", "/api/broadcast/**", "/api/config/is_cloud",
                        "/api/system/license/file", "/api/system/license/content", "/api/system/license/trial",
                        "/api/system/license", "/api/system/diag/progress", "/api/system/roll_event_log",
                        "/api/user/authentication*/**", "/api/query/history_queries/table_names", "/api/models/model_info",
                        "/api/**/metrics", "/api/system/backup", "/api/jobs/spark", "/api/jobs/stage/status", "/api/jobs/error",
                        "/api/jobs/wait_and_run_time", "/api/cache*/**", "/api/admin/public_config",
                        "/api/admin/instance_info", "/api/projects", "/api/system/license/info")
                .permitAll()
                .and()
                .authorizeRequests()
                .antMatchers("/api/monitor/alert", "/api/admin*/**")
                .access("hasRole('ROLE_ADMIN')")
                .and()
                .authorizeRequests()
                .antMatchers("/api/cubes/src/tables")
                .access("hasRole('ROLE_ANALYST')")
                .and()
                .authorizeRequests()
                .antMatchers("/api/query*/**", "/api/metadata*/**", "/api/cubes*/**", "/api/models*/**", "/api/job*/**",
                        "/api/**")
                .authenticated();

        http.logout()
                .invalidateHttpSession(true)
                .deleteCookies("JSESSIONID")
                .logoutUrl("/api/j_spring_security_logout")
                .logoutSuccessHandler(logoutSuccessHandler);

        http.sessionManagement(configurer -> configurer.sessionFixation().newSession());
        // @formatter:on
    }
}