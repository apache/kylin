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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_GROUP_NOT_EXIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.AclRecord;
import org.apache.kylin.rest.security.CompositeAclPermission;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.UserAclManager;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
@PrepareForTest({ SpringContext.class, UserGroupInformation.class, KylinConfig.class, NProjectManager.class })
public class AccessServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    AccessService accessService = Mockito.spy(AccessService.class);

    @InjectMocks
    ProjectService projectService = Mockito.spy(ProjectService.class);

    @InjectMocks
    private IUserGroupService userGroupService = Mockito.spy(IUserGroupService.class);

    @Mock
    AclService aclService = Mockito.spy(AclService.class);

    @Mock
    UserService userService = Mockito.spy(UserService.class);

    @Mock
    UserAclService userAclService = Mockito.spy(UserAclService.class);

    @Mock
    AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(SpringContext.class);

        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata("src/test/resources/ut_access");
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(aclEvaluate, "userAclService", userAclService);
        ReflectionTestUtils.setField(userAclService, "userService", userService);
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");

        // Init users
        ManagedUser adminUser = new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                new SimpleGrantedAuthority(Constant.ROLE_MODELER)));
        ManagedUser modelerUser = new ManagedUser("MODELER", "MODELER", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ANALYST), new SimpleGrantedAuthority(Constant.ROLE_MODELER)));
        ManagedUser analystUser = new ManagedUser("ANALYST", "ANALYST", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ANALYST)));

        List<ManagedUser> users = Lists.newArrayList(adminUser, modelerUser, analystUser);

        Mockito.when(userService.listUsers()).thenReturn(users);
        Mockito.when(userService.loadUserByUsername("ADMIN")).thenReturn(adminUser);
        Mockito.when(userService.loadUserByUsername("MODELER")).thenReturn(modelerUser);
        Mockito.when(userService.loadUserByUsername("ANALYST")).thenReturn(analystUser);
        Mockito.when(userService.userExists("ADMIN")).thenReturn(true);
        Mockito.when(userService.userExists("MODELER")).thenReturn(true);
        Mockito.when(userService.userExists("ANALYST")).thenReturn(true);
        Mockito.when(userService.getGlobalAdmin()).thenReturn(Sets.newHashSet("ADMIN"));
        Mockito.when(userService.listSuperAdminUsers()).thenReturn(Lists.newArrayList("ADMIN"));

        // for SpringContext.getBean() in AclManager

        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenReturn(applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenReturn(PowerMockito.mock(PermissionFactory.class));
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenReturn(PowerMockito.mock(PermissionGrantingStrategy.class));
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        Sid adminSid = accessService.getSid("ADMIN", true);
        Assert.assertNotNull(adminSid);
        Assert.assertNotNull(AclPermissionFactory.getPermissions());

        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        accessService.clean(ae, true);
        Assert.assertThrows(KylinException.class, () -> accessService.clean(null, true));
        AclEntity attachedEntity = new AclServiceTest.MockAclEntity("attached-domain-object");
        accessService.clean(attachedEntity, true);

        // test getAcl
        Acl acl = accessService.getAcl(ae);
        Assert.assertNull(acl);

        // test init
        acl = accessService.init(ae, AclPermission.ADMINISTRATION);
        assertEquals("ADMIN", ((PrincipalSid) acl.getOwner()).getPrincipal());
        assertEquals(1, accessService.generateAceResponses(acl).size());
        AccessEntryResponse aer = accessService.generateAceResponses(acl).get(0);
        assertTrue(CollectionUtils.isEmpty(aer.getExtPermissions()));
        checkResult(ae, aer);

        // test grant
        Sid modeler = accessService.getSid("MODELER", true);
        acl = accessService.grant(ae, AclPermission.ADMINISTRATION, modeler);
        assertEquals(2, accessService.generateAceResponses(acl).size());

        Assert.assertThrows(KylinException.class,
                () -> accessService.grant(null, AclPermission.ADMINISTRATION, modeler));
        Assert.assertThrows(KylinException.class, () -> accessService.grant(ae, null, modeler));
        Assert.assertThrows(KylinException.class, () -> accessService.grant(ae, AclPermission.ADMINISTRATION, null));

        int modelerEntryId = 0;
        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                Assert.assertSame(AclPermission.ADMINISTRATION, ace.getPermission());
            }
        }

        // test update
        acl = accessService.update(ae, modelerEntryId, AclPermission.READ);

        assertEquals(2, accessService.generateAceResponses(acl).size());

        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                Assert.assertSame(AclPermission.READ, ace.getPermission());
            }
        }

        acl = accessService.update(ae, modelerEntryId,
                new CompositeAclPermission(AclPermission.READ, Arrays.asList(AclPermission.DATA_QUERY)));
        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                assertTrue(AclPermissionUtil.hasQueryPermission(ace.getPermission()));
            }
        }

        accessService.clean(attachedEntity, true);

        Acl attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNull(attachedEntityAcl);
        attachedEntityAcl = accessService.init(attachedEntity, AclPermission.ADMINISTRATION);

        accessService.inherit(attachedEntity, ae);

        Assert.assertThrows(KylinException.class, () -> accessService.inherit(null, ae));
        Assert.assertThrows(KylinException.class, () -> accessService.inherit(attachedEntity, null));

        attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNotNull(attachedEntityAcl.getParentAcl());
        assertEquals("test-domain-object", attachedEntityAcl.getParentAcl().getObjectIdentity().getIdentifier());
        assertEquals(1, attachedEntityAcl.getEntries().size());

        // test revoke
        acl = accessService.revoke(ae, modelerEntryId);
        assertEquals(1, accessService.generateAceResponses(acl).size());

        // test clean
        accessService.clean(ae, true);
        acl = accessService.getAcl(ae);
        Assert.assertNull(acl);

        attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNull(attachedEntityAcl);
    }

    @Test
    public void testCompositeAclPermission() {
        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        Acl acl = accessService.getAcl(ae);
        Assert.assertNull(acl);
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        Sid modeler = accessService.getSid("MODELER", true);
        acl = accessService.grant(ae, AclPermission.MANAGEMENT, modeler);
        Assert.assertEquals(32, acl.getEntries().get(0).getPermission().getMask());

        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
        acl = accessService.grant(ae, AclPermission.MANAGEMENT, modeler);
        Assert.assertEquals(160, acl.getEntries().get(0).getPermission().getMask());
    }

    @Test
    public void testUpdateExtensionPermissionException() {
        Assert.assertThrows(MsgPicker.getMsg().getAclDomainNotFound(), KylinException.class,
                () -> accessService.updateExtensionPermission(null, null));
        UserAclManager.getInstance(getTestConfig()).deletePermission("admin", AclPermission.DATA_QUERY);
        Mockito.when(userService.listSuperAdminUsers()).thenReturn(Collections.emptyList());
        ThrowingRunnable func = () -> accessService.updateExtensionPermission(new AclRecord(), null);
        Assert.assertThrows(MsgPicker.getMsg().getAclPermissionRequired(), KylinException.class, func);
    }

    private void checkResult(AclEntity ae, AccessEntryResponse aer) {
        Assert.assertNotNull(aer.getId());
        Assert.assertSame(AclPermission.ADMINISTRATION, aer.getPermission());
        assertEquals("ADMIN", ((PrincipalSid) aer.getSid()).getPrincipal());

        // test init twice
        assertThrows(KylinException.class, () -> accessService.init(ae, AclPermission.ADMINISTRATION));
    }

    @Test
    public void testBatchGrantAndRevokeException() {
        AclEntity ae = new AclServiceTest.MockAclEntity("batch-grant");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        Assert.assertThrows(KylinException.class, () -> accessService.batchGrant(null, sidToPerm));
        Assert.assertThrows(KylinException.class, () -> accessService.batchGrant(ae, null));
    }

    @Test
    public void testBatchGrantAndRevoke() {
        AclEntity ae = new AclServiceTest.MockAclEntity("batch-grant");
        // data-permission-default-enabled:true
        {
            getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
            List<AccessRequest> requests = Lists.newArrayList();
            for (int i = 0; i < 10; i++) {
                requests.add(createAccessRequest("u" + i, "ADMINISTRATION"));
            }

            accessService.batchGrant(requests, ae);
            MutableAclRecord acl = accessService.getAcl(ae);
            List<AccessControlEntry> e = acl.getEntries();
            assertEquals(10, e.size());
            for (int i = 0; i < e.size(); i++) {
                assertEquals(new PrincipalSid("u" + i), e.get(i).getSid());
                assertTrue(AclPermissionUtil.hasQueryPermission(e.get(0).getPermission()));
            }

            //test batch revoke
            accessService.batchRevoke(ae, requests);
            acl = accessService.getAcl(ae);
            e = acl.getEntries();
            assertEquals(0, e.size());
        }
        // data-permission-default-enabled:false
        {
            getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
            List<AccessRequest> requests = Lists.newArrayList(createAccessRequest("u1", "ADMINISTRATION"));

            accessService.batchGrant(requests, ae);
            MutableAclRecord acl = accessService.getAcl(ae);
            List<AccessControlEntry> e = acl.getEntries();
            assertEquals(1, e.size());
            assertEquals(new PrincipalSid("u1"), e.get(0).getSid());
            assertFalse(AclPermissionUtil.hasQueryPermission(e.get(0).getPermission()));

            accessService.batchRevoke(ae, requests);
        }
        {
            List<AccessRequest> requests = Lists.newArrayList(createAccessRequest("u1", ""));
            accessService.batchGrant(requests, ae);
            MutableAclRecord acl = accessService.getAcl(ae);
            List<AccessControlEntry> e = acl.getEntries();
            assertEquals(0, e.size());
        }

        // for group ALL_USERS
        Sid sid = new GrantedAuthoritySid("ALL_USERS");
        accessService.batchGrant(ae, Collections.singletonMap(sid,
                new CompositeAclPermission(AclPermission.MANAGEMENT, Arrays.asList(AclPermission.DATA_QUERY))));
        Assert.assertTrue(AclPermissionUtil.isSpecificPermissionInProject("ALL_USERS", AclPermission.DATA_QUERY,
                accessService.getAcl(ae)));

        thrown.expect(KylinException.class);
        accessService.batchRevoke(null, Collections.emptyList());
    }

    private AccessRequest createAccessRequest(String sid, String permission) {
        AccessRequest request = new AccessRequest();
        request.setSid(sid);
        request.setPrincipal(true);
        request.setPermission(permission);
        return request;
    }

    @Test(expected = KylinException.class)
    public void testCheckGlobalAdminException() throws IOException {
        accessService.checkGlobalAdmin("ADMIN");
    }

    @Test
    public void testCheckGlobalAdmin() throws IOException {
        accessService.checkGlobalAdmin("ANALYSIS");
        accessService.checkGlobalAdmin(Arrays.asList("ANALYSIS", "MODEL", "AAA"));
    }

    @Test
    public void testGenerateAceResponsesByFuzzMatching() throws Exception {
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("admin"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ANALYST"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ROLE_ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("role_ADMIN"), AclPermission.ADMINISTRATION);
        accessService.batchGrant(ae, sidToPerm);
        Mockito.when(userGroupService.exists(Mockito.anyString())).thenReturn(true);
        Mockito.when(userService.userExists(Mockito.anyString())).thenReturn(true);
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        assertEquals(2, result.size());
        assertEquals("ANALYST", ((PrincipalSid) result.get(0).getSid()).getPrincipal());
    }

    @Test
    public void testGenerateAceResponsesByFuzzMatchingWhenHasSameNameUserAndGroupName() throws Exception {
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        Mockito.when(userGroupService.exists("ADMIN")).thenReturn(true);
        sidToPerm.put(new GrantedAuthoritySid("grp1"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        accessService.batchGrant(ae, sidToPerm);
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        assertEquals(1, result.size());
        // expect ADMIN group is in acl
        assertEquals("ADMIN", ((GrantedAuthoritySid) result.get(0).getSid()).getGrantedAuthority());
    }

    @Test
    public void testGetProjectAdminUsers() throws IOException {
        String project = "default";
        Set<String> result = accessService.getProjectAdminUsers(project);
        assertEquals(1, result.size());
    }

    @Test
    public void testGetProjectManagementUsers() throws IOException {
        String project = "default";
        Set<String> result = accessService.getProjectManagementUsers(project);
        assertEquals(1, result.size());
    }

    @Test
    public void testHasProjectAdminPermission() {
        MutableAclRecord ace = AclPermissionUtil.getProjectAcl("default");
        aclService.upsertAce(ace, new PrincipalSid("czw9976"), AclPermission.ADMINISTRATION);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("czw9976", "czw9976", Constant.ROLE_MODELER));
        Assert.assertTrue(AclPermissionUtil.hasProjectAdminPermission("default", accessService.getCurrentUserGroups()));
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }

    @Test
    public void testCleanupProjectAcl() throws Exception {
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("admin"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ANALYST"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ROLE_ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("role_ADMIN"), AclPermission.ADMINISTRATION);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            accessService.batchGrant(ae, sidToPerm);
            return null;
        }, UnitOfWork.GLOBAL_UNIT);

        projectService.cleanupAcl();
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        assertEquals(0, result.size());
    }

    @Test
    public void testRevokeWithSid() {
        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        accessService.init(ae, AclPermission.ADMINISTRATION);

        Sid modeler = accessService.getSid("MODELER", true);
        accessService.grant(ae, AclPermission.ADMINISTRATION, modeler);

        Acl acl = accessService.revokeWithSid(ae, "MODELER", true);
        assertEquals(1, accessService.generateAceResponses(acl).size());

        thrown.expect(KylinException.class);
        accessService.revokeWithSid(null, "MODELER", true);
    }

    @Test
    public void testGetCurrentUserPermissionInProject() throws IOException {
        String result = accessService.getCurrentUserPermissionInProject("default");
        assertEquals("ADMIN", result);
    }

    @Test
    public void testAdminUserExtPermissionInProject() {
        // super admin
        assertTrue(accessService.getUserNormalExtPermissions("default").contains("DATA_QUERY"));
        aclEvaluate.checkProjectQueryPermission("default");

        // set admin as a non-super admin
        Mockito.when(userService.listSuperAdminUsers()).thenReturn(Collections.emptyList());

        // system admin without data query permission
        assertFalse(accessService.getUserNormalExtPermissions("default").contains("DATA_QUERY"));
        Assert.assertThrows(AccessDeniedException.class, () -> aclEvaluate.checkProjectQueryPermission("default"));

        // system admin with global data query permission
        userAclService.getManager(UserAclManager.class).addPermission("ADMIN", AclPermission.DATA_QUERY);
        assertTrue(accessService.getUserNormalExtPermissions("default").contains("DATA_QUERY"));
        aclEvaluate.checkProjectQueryPermission("default");
        userAclService.getManager(UserAclManager.class).deletePermission("ADMIN", AclPermission.DATA_QUERY);

        // system admin with data query permission for special project
        assertFalse(accessService.getUserNormalExtPermissions("default").contains("DATA_QUERY"));

        userAclService.getManager(UserAclManager.class).addDataQueryProject("ADMIN", "default");
        Mockito.when(userAclService.canAdminUserQuery(Mockito.anyString())).thenReturn(false);
        assertTrue(accessService.getUserNormalExtPermissions("default").contains("DATA_QUERY"));
        aclEvaluate.checkProjectQueryPermission("default");
        userAclService.getManager(UserAclManager.class).deleteDataQueryProject("ADMIN", "default");

        // system admin with group which has query permission for special project
        assertFalse(accessService.getUserNormalExtPermissions("default").contains("DATA_QUERY"));

        String projectUuid = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default")
                .getUuid();
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
        accessService.grant(ae, AclPermission.READ, new GrantedAuthoritySid("ROLE_ADMIN"));
        assertTrue(accessService.getUserNormalExtPermissions("default").contains("DATA_QUERY"));
        aclEvaluate.checkProjectQueryPermission("default");
    }

    @Test
    public void testExtPermissionInProject() {
        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        accessService.init(ae, AclPermission.ADMINISTRATION);

        Sid modeler = accessService.getSid("MODELER", true);
        Acl acl = accessService.grant(ae, AclPermission.ADMINISTRATION, modeler);

        int modelerEntryId = 0;
        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                Assert.assertSame(AclPermission.ADMINISTRATION, ace.getPermission());
            }
        }

        // test update Extension Permission
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid("MODELER");
        accessRequest.setPrincipal(true);
        accessRequest.setAccessEntryId(modelerEntryId);
        accessRequest.setExtPermissions(Collections.singletonList("DATA_QUERY"));
        acl = accessService.updateExtensionPermission(ae, accessRequest);
        assertEquals(2, accessService.generateAceResponses(acl).size());

        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                assertInstanceOf(CompositeAclPermission.class, ace.getPermission());
                Assert.assertTrue(((CompositeAclPermission) ace.getPermission()).getExtPermissions()
                        .contains(AclPermission.DATA_QUERY));
            }
        }

        // test delete DATA_QUERY permission
        AccessRequest accessRequestd = new AccessRequest();
        accessRequestd.setSid("MODELER");
        accessRequestd.setPrincipal(true);
        accessRequestd.setExtPermissions(Collections.emptyList());
        acl = accessService.updateExtensionPermission(ae, accessRequestd);
        assertEquals(2, accessService.generateAceResponses(acl).size());

        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                assertNotEquals(CompositeAclPermission.class, ace.getPermission().getClass());
            }
        }

        // test get User Ext Permissions
        accessService.updateExtensionPermission(ae, accessRequest);
        assertTrue(accessService.getUserNormalExtPermissions("test-domain-object", "MODELER").contains(128));

        PowerMockito.mockStatic(KylinConfig.class);
        PowerMockito.mockStatic(NProjectManager.class);
        KylinConfig kylinConfig = mock(KylinConfig.class);
        NProjectManager nProjectManager = mock(NProjectManager.class);
        ProjectInstance projectInstance = mock(ProjectInstance.class);
        PowerMockito.when(KylinConfig.getInstanceFromEnv()).thenReturn(kylinConfig);
        PowerMockito.when(NProjectManager.getInstance(any())).thenReturn(nProjectManager);
        when(nProjectManager.getProject(anyString())).thenReturn(projectInstance);
        when(projectInstance.getUuid()).thenReturn("test-domain-object");
        assertTrue(accessService.getUserNormalExtPermissions("test-domain-object").contains("DATA_QUERY"));

    }

    @Test
    public void testGetGrantedProjectsOfUser() throws IOException {
        List<String> result = accessService.getGrantedProjectsOfUser("ADMIN");
        assertEquals(34, result.size());
    }

    @Test
    public void testGetGrantedProjectsOfUserOrGroup() throws IOException {
        // admin user
        List<String> result = accessService.getGrantedProjectsOfUserOrGroup("ADMIN", true);
        assertEquals(34, result.size());

        // normal user
        result = accessService.getGrantedProjectsOfUserOrGroup("ANALYST", true);
        assertEquals(0, result.size());

        // granted admin group
        addGroupAndGrantPermission("ADMIN_GROUP", AclPermission.ADMINISTRATION);
        Mockito.when(userGroupService.exists("ADMIN_GROUP")).thenReturn(true);
        result = accessService.getGrantedProjectsOfUserOrGroup("ADMIN_GROUP", false);
        assertEquals(1, result.size());

        // granted normal group
        addGroupAndGrantPermission("MANAGEMENT_GROUP", AclPermission.MANAGEMENT);
        Mockito.when(userGroupService.exists("MANAGEMENT_GROUP")).thenReturn(true);
        result = accessService.getGrantedProjectsOfUserOrGroup("MANAGEMENT_GROUP", false);
        assertEquals(1, result.size());

        // does not grant, normal group
        userGroupService.addGroup("NORMAL_GROUP");
        Mockito.when(userGroupService.exists("NORMAL_GROUP")).thenReturn(true);
        result = accessService.getGrantedProjectsOfUserOrGroup("NORMAL_GROUP", false);
        assertEquals(0, result.size());

        // not exist user
        thrown.expectMessage("Operation failed, user:[nouser] not exists, please add it first");
        accessService.getGrantedProjectsOfUser("nouser");
    }

    @Test
    public void testGetGrantedProjectsOfUserOrGroupWithNotExistGroup() throws IOException {
        thrown.expectMessage(USER_GROUP_NOT_EXIST.getMsg("nogroup"));
        accessService.getGrantedProjectsOfUserOrGroup("nogroup", false);
    }

    private void addGroupAndGrantPermission(String group, Permission permission) throws IOException {
        ProjectInstance projectInstance = NProjectManager.getInstance(getTestConfig()).getProject("default");
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        userGroupService.addGroup(group);
        Sid sid = accessService.getSid(group, false);
        accessService.grant(ae, permission, sid);
    }

    @Test
    public void testCheckAccessRequestList() throws IOException {
        Mockito.when(userGroupService.getAllUserGroups()).thenReturn(Arrays.asList("ALL_USERS", "MANAGEMENT"));

        List<AccessRequest> accessRequests = new ArrayList<>();
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(0);
        accessRequest.setPermission("MANAGEMENT");
        accessRequest.setSid("ANALYST");
        accessRequest.setPrincipal(true);
        accessRequests.add(accessRequest);
        accessService.checkAccessRequestList(accessRequests);

        AccessRequest accessRequest2 = new AccessRequest();
        accessRequest2.setAccessEntryId(0);
        accessRequest2.setPermission("ADMIN");
        accessRequest2.setSid("ADMIN");
        accessRequest2.setPrincipal(true);
        accessRequests.add(accessRequest2);
        thrown.expectMessage("You cannot add,modify or remove the system administrator’s rights");
        accessService.checkAccessRequestList(accessRequests);
    }

    @Test
    public void testCheckSid() throws IOException {
        accessService.checkSid(new ArrayList<>());

        thrown.expectMessage("User/Group name should not be empty.");

        List<AccessRequest> accessRequests = new ArrayList<>();
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(0);
        accessRequest.setPermission("MANAGEMENT");
        accessRequest.setSid("ANALYST");
        accessRequest.setPrincipal(true);
        accessRequests.add(accessRequest);
        accessService.checkSid(accessRequests);

        Mockito.when(userGroupService.getAllUserGroups()).thenReturn(Arrays.asList("ALL_USERS", "MANAGEMENT"));

        accessService.checkSid("ADMIN", true);

        thrown.expectMessage("User/Group name should not be empty.");
        accessService.checkSid("", true);
    }

    @Test
    public void testCheckEmptySid() {
        accessService.checkSidNotEmpty("ADMIN", true);

        thrown.expectMessage("User/Group name should not be empty.");
        accessService.checkSidNotEmpty("", true);
    }

    @Test
    public void testCheckSidWithEmptyUser() throws IOException {
        thrown.expectMessage("User/Group name should not be empty.");
        accessService.checkSid("", false);
    }

    @Test
    public void testCheckSidWithNotExistUser() throws IOException {
        thrown.expectMessage("Operation failed, user:[nouser] not exists, please add it first");
        accessService.checkSid("nouser", true);
    }

    @Test
    public void testCheckSidWithNotExistGroup() throws IOException {
        thrown.expectMessage(USER_GROUP_NOT_EXIST.getMsg("nogroup"));
        accessService.checkSid("nogroup", false);
    }

    @Test
    public void testIsGlobalAdmin() throws IOException {
        boolean result = accessService.isGlobalAdmin("ADMIN");
        assertTrue(result);

        result = accessService.isGlobalAdmin("ANALYST");
        Assert.assertFalse(result);
    }

    @Test
    public void testGetGroupsOfCurrentUser() {
        List<String> result = accessService.getGroupsOfCurrentUser();
        assertEquals(4, result.size());
    }

    @Test
    public void testGetProjectUsersAndGroups() throws IOException {
        Mockito.when(userService.getGlobalAdmin()).thenReturn(Sets.newHashSet("ADMIN", "CCL6911"));
        assertTrue(userService.getGlobalAdmin().contains("CCL6911"));
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE,
                "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");
        Map<String, List<String>> map = accessService.getProjectUsersAndGroups(ae);
        assertEquals(2, map.get("user").size());
        assertEquals(1, map.get("group").size());
        assertTrue(map.get("user").contains("ADMIN"));
        assertTrue(map.get("group").contains("ROLE_ADMIN"));
    }

    @Test
    public void testAclWithUnNaturalOrder() {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE,
                "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");

        // read from metadata
        MutableAclRecord acl = accessService.getAcl(ae);
        // order by sid_order in aceImpl
        // ADL6911(group), BDL6911(group), aCL6911(group), ACZ5815(user), ACZ5815(user), czw9976(user)
        List<AccessControlEntry> entries = acl.getEntries();

        checkEntries(entries);

        // grant
        acl = accessService.grant(ae, BasePermission.ADMINISTRATION, accessService.getSid("atest1", true));

        entries = acl.getEntries();
        assertEquals(7, entries.size());
        assertEquals(BasePermission.ADMINISTRATION, entries.get(5).getPermission());
        assertFalse(AclPermissionUtil.hasQueryPermission(entries.get(5).getPermission()));

        assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        assertEquals("atest1", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());
        assertEquals("czw9976", ((PrincipalSid) entries.get(6).getSid()).getPrincipal());

        // revoke czw9976
        acl = accessService.revoke(ae, 6);
        Assert.assertThrows(KylinException.class, () -> accessService.revoke(null, 6));

        entries = acl.getEntries();
        checkAcl(entries);

    }

    @Test
    public void testAclWithUnNaturalOrderUpdate() throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE,
                "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");

        Mockito.when(userService.userExists(Mockito.anyString())).thenReturn(true);
        Mockito.when(userGroupService.exists(Mockito.anyString())).thenReturn(true);
        // read from metadata
        MutableAclRecord acl = accessService.getAcl(ae);
        // order by sid_order in aceImpl
        // ADL6911(group), BDL6911(group), aCL6911(group), ACZ5815(user), ACZ5815(user), czw9976(user)
        List<AccessControlEntry> entries = acl.getEntries();

        checkEntries(entries);

        // grant
        acl = accessService.grant(ae, BasePermission.ADMINISTRATION, accessService.getSid("atest1", true));

        entries = acl.getEntries();

        // update atest1
        assertEquals(BasePermission.ADMINISTRATION, entries.get(5).getPermission());

        acl = accessService.update(ae, 5, BasePermission.READ);
        entries = acl.getEntries();
        assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        assertEquals("atest1", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());
        assertEquals(BasePermission.READ, entries.get(5).getPermission());

        acl = accessService.update(ae, 1,
                new CompositeAclPermission(BasePermission.ADMINISTRATION, Arrays.asList(AclPermission.DATA_QUERY)));
        entries = acl.getEntries();
        assertTrue(AclPermissionUtil.hasQueryPermission(entries.get(1).getPermission()));
        assertTrue(AclPermissionUtil.hasExtPermission(entries.get(1).getPermission()));
        assertEquals(144, entries.get(1).getPermission().getMask());
        assertEquals(BasePermission.ADMINISTRATION,
                AclPermissionUtil.convertToBasePermission(entries.get(1).getPermission()));
        assertTrue(AclPermissionUtil.convertToCompositePermission(entries.get(1).getPermission()).getExtPermissions()
                .contains(AclPermission.DATA_QUERY));
        AccessEntryResponse aer = accessService.generateAceResponses(acl).get(1);
        assertEquals(AclPermission.DATA_QUERY, aer.getExtPermissions().get(0));

        Assert.assertThrows(KylinException.class, () -> accessService.update(null, 5, AclPermission.DATA_QUERY));
        Assert.assertThrows(KylinException.class, () -> accessService.update(ae, 5, null));

    }

    private void checkAcl(List<AccessControlEntry> entries) {
        assertEquals(6, entries.size());

        assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        assertEquals("atest1", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());
    }

    private void checkEntries(List<AccessControlEntry> entries) {
        assertEquals(6, entries.size());

        assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        assertEquals("czw9976", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());
    }

    @Test
    public void testSetPermissions() {
        Sid sid = new PrincipalSid("user1");
        AccessEntryResponse accessEntryResponse = new AccessEntryResponse("1L", sid, BasePermission.ADMINISTRATION,
                true);
        CompositeAclPermission compositeAclPermission = new CompositeAclPermission(AclPermission.MANAGEMENT);
        compositeAclPermission.addExtPermission(AclPermission.DATA_QUERY);
        Assert.assertTrue(compositeAclPermission.getExtMasks().contains(AclPermission.DATA_QUERY.getMask()));
        accessEntryResponse.setPermission(compositeAclPermission);
        Assert.assertEquals(AclPermission.MANAGEMENT, accessEntryResponse.getPermission());
        Assert.assertTrue(accessEntryResponse.getExtPermissions().contains(AclPermission.DATA_QUERY));

    }

    @Test
    public void testBatchCheckSidWithEmptyGroup() throws IOException {
        thrown.expectMessage("User/Group name should not be empty.");
        accessService.batchCheckSid("", false, null);
    }

    @Test
    public void testBatchCheckSidWithEmptyUser() throws IOException {
        thrown.expectMessage("User/Group name should not be empty.");
        accessService.batchCheckSid("", true, null);
    }

    @Test
    public void testBatchCheckSidWithEmptyAllGroups() throws IOException {
        thrown.expectMessage("User/Group name should not be empty.");
        accessService.batchCheckSid("group1", false, null);
    }

    @Test
    public void testBatchCheckSidWithNotExistUser() throws IOException {
        thrown.expectMessage("Operation failed, user:[nouser] not exists, please add it first");
        accessService.batchCheckSid("nouser", true, null);
    }

    @Test
    public void testBatchCheckSidWithNotExistGroup() throws IOException {
        thrown.expectMessage(USER_GROUP_NOT_EXIST.getMsg("nogroup"));
        List<String> existed = Arrays.asList("group1", "group2");
        accessService.batchCheckSid("nogroup", false, existed);
    }

    @Test
    public void testCheckProjectOperationDesignPermission() {
        getTestConfig().setProperty("kylin.index.enable-operator-design", "true");
        String username = "OPERATION1";
        ManagedUser managedUser = new ManagedUser(username, username, false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS)));
        if (!userService.userExists(username)) {
            userService.createUser(managedUser);
        }

        ProjectInstance projectInstance = NProjectManager.getInstance(getTestConfig()).getProject("default");
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        Sid sid = accessService.getSid(username, false);
        accessService.grant(ae, AclPermission.OPERATION, sid);

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken(username, username, Constant.GROUP_ALL_USERS));

        aclEvaluate.checkProjectOperationDesignPermission("default");

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }
}
