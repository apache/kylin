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

package org.apache.kylin.helper;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import javax.naming.directory.SearchControls;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.metadata.upgrade.GlobalAclVersionManager;
import org.apache.kylin.tool.util.LdapUtils;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;

import lombok.val;

public class UpdateUserAclToolHelper {
    private UpdateUserAclToolHelper() {
    }

    public static UpdateUserAclToolHelper getInstance() {
        return new UpdateUserAclToolHelper();
    }

    public Set<String> getLdapAdminUsers() {
        val ldapTemplate = createLdapTemplate();
        val ldapUserDNs = LdapUtils.getAllGroupMembers(ldapTemplate,
                KylinConfig.getInstanceFromEnv().getLDAPAdminRole());
        val searchControls = new SearchControls();
        searchControls.setSearchScope(2);
        Map<String, String> dnMapperMap = LdapUtils.getAllValidUserDnMap(ldapTemplate, searchControls);
        val users = new HashSet<String>();
        for (String u : ldapUserDNs) {
            Optional.ofNullable(dnMapperMap.get(u)).ifPresent(users::add);
        }
        return users;
    }

    private SpringSecurityLdapTemplate createLdapTemplate() {
        val properties = KylinConfig.getInstanceFromEnv().exportToProperties();
        val contextSource = new DefaultSpringSecurityContextSource(
                properties.getProperty("kylin.security.ldap.connection-server"));
        contextSource.setUserDn(properties.getProperty("kylin.security.ldap.connection-username"));
        contextSource.setPassword(getPassword(properties));
        contextSource.afterPropertiesSet();
        return new SpringSecurityLdapTemplate(contextSource);
    }

    public String getPassword(Properties properties) {
        val password = properties.getProperty("kylin.security.ldap.connection-password");
        return EncryptUtil.decrypt(password);
    }

    public boolean isUpgraded() {
        val versionManager = GlobalAclVersionManager.getInstance(KylinConfig.getInstanceFromEnv());
        return versionManager.exists();
    }

}
