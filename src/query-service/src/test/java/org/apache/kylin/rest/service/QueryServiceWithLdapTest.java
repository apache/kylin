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

import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.ldap.test.unboundid.LdapTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextHierarchy({ @ContextConfiguration(locations = { "classpath:applicationContext.xml" }),
        @ContextConfiguration(locations = { "classpath:kylinSecurity.xml" }) })
@WebAppConfiguration(value = "src/main/resources")
@ActiveProfiles({ "ldap", "ldap-test", "test" })
public class QueryServiceWithLdapTest extends NLocalFileMetadataTestCase {

    private static final String LDAP_CONFIG = "ut_ldap/ldap-config.properties";

    private static final String LDAP_SERVER = "ut_ldap/ldap-server.ldif";

    private static InMemoryDirectoryServer directoryServer;
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Autowired
    @Qualifier("userService")
    LdapUserService ldapUserService;
    @Autowired
    @Qualifier("userGroupService")
    LdapUserGroupService userGroupService;
    @Autowired
    @Qualifier("queryService")
    QueryService queryService;
    @Autowired
    private WebApplicationContext context;
    private MockMvc mockMvc;

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
        Properties ldapConfig = new Properties();
        ldapConfig.load(new FileInputStream(new ClassPathResource(LDAP_CONFIG).getFile()));
        final KylinConfig kylinConfig = getTestConfig();
        overwriteSystemPropBeforeClass("kylin.security.ldap.max-page-size", "1");
        ldapConfig.forEach((k, v) -> kylinConfig.setProperty(k.toString(), v.toString()));

        String dn = ldapConfig.getProperty("kylin.security.ldap.connection-username");
        String password = ldapConfig.getProperty("kylin.security.ldap.connection-password");
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
        config.addAdditionalBindCredentials(dn, EncryptUtil.decrypt(password));
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("LDAP", 8389));
        config.setEnforceSingleStructuralObjectClass(false);
        config.setEnforceAttributeSyntaxCompliance(true);
        config.setMaxSizeLimit(1);
        directoryServer = new InMemoryDirectoryServer(config);
        directoryServer.startListening();
        log.info("current directory server listen on {}", directoryServer.getListenPort());
        LdapTestUtils.loadLdif(directoryServer, new ClassPathResource(LDAP_SERVER));
    }

    @AfterClass
    public static void cleanupResource() throws Exception {
        directoryServer.shutDown(true);
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
    }

    @After
    public void cleanup() {
    }

    @Test
    public void testCollectQueryScanRowsAndTimeCondition() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            boolean isEffective;
            config.setProperty("kylin.query.auto-adjust-big-query-rows-threshold-enabled", "false");
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertFalse(isEffective);
            config.setProperty("kylin.query.auto-adjust-big-query-rows-threshold-enabled", "true");
            QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertFalse(isEffective);
            QueryContext.current().getQueryTagInfo().setAsyncQuery(false);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertTrue(isEffective);
            QueryContext.current().getQueryTagInfo().setStorageCacheUsed(true);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertFalse(isEffective);
            QueryContext.current().getQueryTagInfo().setStorageCacheUsed(false);
            isEffective = queryService.isCollectQueryScanRowsAndTimeEnabled();
            Assert.assertTrue(isEffective);
        }
    }

    @Test
    public void testGetLdapUserACLInfo() {
        {
            ldapUserService.listUsers();
            ldapUserService.loadUserByUsername("jenny");
            QueryContext.AclInfo info = queryService.getExecuteAclInfo("default", "jenny");
            Assert.assertTrue(info.getGroups().contains("ROLE_ADMIN"));
        }

        {
            ldapUserService.loadUserByUsername("johnny");
            QueryContext.AclInfo info = queryService.getExecuteAclInfo("default", "johnny");
            Assert.assertFalse(info.getGroups().contains("ROLE_ADMIN"));
        }
    }

}
