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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;

@Profile({ "ldap", "saml" })
@Configuration
public class LdapSamlProfileConfiguration {

    @Value("${kylin.security.ldap.connection-server}")
    private String connectionServer;

    @Value("${kylin.security.ldap.connection-username}")
    private String connectionUsername;

    @Value("${kylin.security.ldap.connection-password}")
    private String connectionPassword;

    @Bean("ldapSource")
    public LdapContextSource ldapSource() {
        DefaultSpringSecurityContextSource ldapSource = new DefaultSpringSecurityContextSource(connectionServer);
        ldapSource.setUserDn(connectionUsername);
        ldapSource.setPassword(connectionPassword);
        return ldapSource;
    }

}