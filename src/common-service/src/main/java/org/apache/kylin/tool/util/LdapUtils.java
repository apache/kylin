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
package org.apache.kylin.tool.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.naming.directory.SearchControls;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.springframework.ldap.control.PagedResultsDirContextProcessor;
import org.springframework.ldap.core.ContextMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.support.SingleContextSource;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LdapUtils {
    private LdapUtils() {

    }

    public static Map<String, String> getAllValidUserDnMap(SpringSecurityLdapTemplate ldapTemplate,
            SearchControls searchControls) {
        String ldapUserSearchBase = KylinConfig.getInstanceFromEnv().getLDAPUserSearchBase();
        String ldapUserSearchFilter = KapConfig.getInstanceFromEnv().getLDAPUserSearchFilter();
        String ldapUserIDAttr = KapConfig.getInstanceFromEnv().getLDAPUserIDAttr();
        Integer maxPageSize = KapConfig.getInstanceFromEnv().getLDAPMaxPageSize();
        log.info("ldap user search config, base: {}, filter: {}, identifier attribute: {}, maxPageSize: {}",
                ldapUserSearchBase, ldapUserSearchFilter, ldapUserIDAttr, maxPageSize);

        final PagedResultsDirContextProcessor processor = new PagedResultsDirContextProcessor(maxPageSize);

        ContextMapper<Pair<String, String>> contextMapper = ctx -> {
            DirContextAdapter adapter = (DirContextAdapter) ctx;
            return Pair.newPair(adapter.getNameInNamespace(),
                    adapter.getAttributes().get(ldapUserIDAttr).get().toString());
        };

        List<Pair<String, String>> pairs = SingleContextSource.doWithSingleContext(ldapTemplate.getContextSource(),
                operations -> {
                    List<Pair<String, String>> pairList = new ArrayList<>();
                    do {
                        pairList.addAll(operations.search(ldapUserSearchBase, ldapUserSearchFilter, searchControls,
                                contextMapper, processor));
                    } while (processor.hasMore());
                    return pairList;
                });

        Map<String, String> resultMap = pairs.stream().collect(Collectors.groupingBy(Pair::getSecond)).entrySet()
                .stream().filter(e -> 1 == e.getValue().size()).map(Map.Entry::getValue).map(list -> list.get(0))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        log.info("LDAP user info load success");
        return resultMap;
    }

    public static Set<String> getAllGroupMembers(SpringSecurityLdapTemplate ldapTemplate, String name) {
        String ldapGroupSearchBase = KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase();
        String ldapGroupMemberSearchFilter = KapConfig.getInstanceFromEnv().getLDAPGroupMemberSearchFilter();
        String ldapGroupMemberAttr = KapConfig.getInstanceFromEnv().getLDAPGroupMemberAttr();
        Set<String> ldapUserDNs = ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                ldapGroupMemberSearchFilter, new String[] { name }, ldapGroupMemberAttr);
        log.info("Ldap group members search config, base: {},member search filter: {}, member identifier: {}",
                ldapGroupSearchBase, ldapGroupMemberSearchFilter, ldapGroupMemberAttr);

        if (ldapUserDNs.isEmpty()) {
            // try using range retrieval
            int left = 0;
            Integer maxValRange = KapConfig.getInstanceFromEnv().getLDAPMaxValRange();
            while (true) {
                String ldapGroupMemberRangeAttr = String.format(Locale.ROOT, "%s;range=%s-%s", ldapGroupMemberAttr,
                        left, left + (maxValRange - 1));
                log.info("Ldap group members search config, base: {},member search filter: {}, member identifier: {}",
                        ldapGroupSearchBase, ldapGroupMemberSearchFilter, ldapGroupMemberRangeAttr);
                Set<String> rangeResults = ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                        ldapGroupMemberSearchFilter, new String[] { name }, ldapGroupMemberRangeAttr);
                if (rangeResults.isEmpty()) {
                    // maybe the last page
                    ldapGroupMemberRangeAttr = String.format(Locale.ROOT, "%s;range=%s-%s", ldapGroupMemberAttr, left,
                            "*");
                    log.info(
                            "Last page, ldap group members search config, base: {},member search filter: {}, member identifier: {}",
                            ldapGroupSearchBase, ldapGroupMemberSearchFilter, ldapGroupMemberRangeAttr);
                    ldapUserDNs.addAll(ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                            ldapGroupMemberSearchFilter, new String[] { name }, ldapGroupMemberRangeAttr));
                    break;
                }
                ldapUserDNs.addAll(rangeResults);
                left += maxValRange;
            }
        }

        return ldapUserDNs;
    }

}
