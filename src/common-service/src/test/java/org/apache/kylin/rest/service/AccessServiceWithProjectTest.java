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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
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

import lombok.val;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
@PrepareForTest({ SpringContext.class, UserGroupInformation.class, KylinConfig.class, NProjectManager.class })
public class AccessServiceWithProjectTest extends NLocalFileMetadataTestCase {

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
    public void testBatchGrantAndRevokeException() {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = new AclServiceTest.MockAclEntity("batch-grant");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        Assert.assertThrows(KylinException.class, () -> accessService.batchGrant(project, null, sidToPerm));
        Assert.assertThrows(KylinException.class, () -> accessService.batchGrant(project, ae, null));
    }

    @Test
    public void testBatchGrantAndRevoke() {
        AclEntity ae = new AclServiceTest.MockAclEntity("batch-grant");
        val project = RandomUtil.randomUUIDStr();
        // data-permission-default-enabled:true
        {
            getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
            List<AccessRequest> requests = Lists.newArrayList();
            for (int i = 0; i < 10; i++) {
                requests.add(createAccessRequest(project, "u" + i, "ADMINISTRATION"));
            }

            accessService.batchGrant(project, requests, ae);
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
            List<AccessRequest> requests = Lists.newArrayList(createAccessRequest(project, "u1", "ADMINISTRATION"));

            accessService.batchGrant(project, requests, ae);
            MutableAclRecord acl = accessService.getAcl(ae);
            List<AccessControlEntry> e = acl.getEntries();
            assertEquals(1, e.size());
            assertEquals(new PrincipalSid("u1"), e.get(0).getSid());
            assertFalse(AclPermissionUtil.hasQueryPermission(e.get(0).getPermission()));

            accessService.batchRevoke(ae, requests);
        }
        {
            List<AccessRequest> requests = Lists.newArrayList(createAccessRequest(project, "u1", ""));
            accessService.batchGrant(project, requests, ae);
            MutableAclRecord acl = accessService.getAcl(ae);
            List<AccessControlEntry> e = acl.getEntries();
            assertEquals(0, e.size());
        }

        // for group ALL_USERS
        Sid sid = new GrantedAuthoritySid("ALL_USERS");
        accessService.batchGrant(project, ae, Collections.singletonMap(sid,
                new CompositeAclPermission(AclPermission.MANAGEMENT, Arrays.asList(AclPermission.DATA_QUERY))));
        Assert.assertTrue(AclPermissionUtil.isSpecificPermissionInProject("ALL_USERS", AclPermission.DATA_QUERY,
                accessService.getAcl(ae)));

        thrown.expect(KylinException.class);
        accessService.batchRevoke(null, Collections.emptyList());
    }

    private AccessRequest createAccessRequest(String project, String sid, String permission) {
        AccessRequest request = new AccessRequest();
        request.setSid(sid);
        request.setPrincipal(true);
        request.setPermission(permission);
        request.setProject(project);
        return request;
    }

    @Test
    public void testGenerateAceResponsesByFuzzMatching() throws Exception {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("admin"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ANALYST"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ROLE_ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("role_ADMIN"), AclPermission.ADMINISTRATION);
        accessService.batchGrant(project, ae, sidToPerm);
        Mockito.when(userGroupService.exists(Mockito.anyString())).thenReturn(true);
        Mockito.when(userService.userExists(Mockito.anyString())).thenReturn(true);
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        assertEquals(2, result.size());
        assertEquals("ANALYST", ((PrincipalSid) result.get(0).getSid()).getPrincipal());
    }

    @Test
    public void testGenerateAceResponsesByFuzzMatchingWhenHasSameNameUserAndGroupName() throws Exception {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        Mockito.when(userGroupService.exists("ADMIN")).thenReturn(true);
        sidToPerm.put(new GrantedAuthoritySid("grp1"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        accessService.batchGrant(project, ae, sidToPerm);
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        assertEquals(1, result.size());
        // expect ADMIN group is in acl
        assertEquals("ADMIN", ((GrantedAuthoritySid) result.get(0).getSid()).getGrantedAuthority());
    }

    @Test
    public void testCleanupProjectAcl() throws Exception {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("admin"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ANALYST"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ROLE_ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("role_ADMIN"), AclPermission.ADMINISTRATION);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            accessService.batchGrant(project, ae, sidToPerm);
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
        projectService.cleanupAcl();
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        assertEquals(0, result.size());
    }

    @Test
    public void testBasics() throws IOException {
        val project = RandomUtil.randomUUIDStr();
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
        acl = accessService.grant(project, ae, AclPermission.ADMINISTRATION, modeler);
        assertEquals(2, accessService.generateAceResponses(acl).size());

        Assert.assertThrows(KylinException.class,
                () -> accessService.grant(project, null, AclPermission.ADMINISTRATION, modeler));
        Assert.assertThrows(KylinException.class, () -> accessService.grant(project, ae, null, modeler));
        Assert.assertThrows(KylinException.class,
                () -> accessService.grant(project, ae, AclPermission.ADMINISTRATION, null));

        int modelerEntryId = 0;
        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                Assert.assertSame(AclPermission.ADMINISTRATION, ace.getPermission());
            }
        }

        // test update
        acl = accessService.update(project, ae, modelerEntryId, AclPermission.READ);

        assertEquals(2, accessService.generateAceResponses(acl).size());

        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                Assert.assertSame(AclPermission.READ, ace.getPermission());
            }
        }

        acl = accessService.update(project, ae, modelerEntryId,
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
        acl = accessService.revoke(project, ae, modelerEntryId);
        assertEquals(1, accessService.generateAceResponses(acl).size());

        // test clean
        accessService.clean(ae, true);
        acl = accessService.getAcl(ae);
        Assert.assertNull(acl);

        attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNull(attachedEntityAcl);
    }

    private void checkResult(AclEntity ae, AccessEntryResponse aer) {
        Assert.assertNotNull(aer.getId());
        Assert.assertSame(AclPermission.ADMINISTRATION, aer.getPermission());
        assertEquals("ADMIN", ((PrincipalSid) aer.getSid()).getPrincipal());

        // test init twice
        assertThrows(KylinException.class, () -> accessService.init(ae, AclPermission.ADMINISTRATION));
    }

    @Test
    public void testCompositeAclPermission() {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        Acl acl = accessService.getAcl(ae);
        Assert.assertNull(acl);
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        Sid modeler = accessService.getSid("MODELER", true);
        acl = accessService.grant(project, ae, AclPermission.MANAGEMENT, modeler);
        Assert.assertEquals(32, acl.getEntries().get(0).getPermission().getMask());

        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
        acl = accessService.grant(project, ae, AclPermission.MANAGEMENT, modeler);
        Assert.assertEquals(160, acl.getEntries().get(0).getPermission().getMask());
    }

    @Test
    public void testRevokeWithSid() {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        accessService.init(ae, AclPermission.ADMINISTRATION);

        Sid modeler = accessService.getSid("MODELER", true);
        accessService.grant(project, ae, AclPermission.ADMINISTRATION, modeler);

        Acl acl = accessService.revokeWithSid(ae, "MODELER", true);
        assertEquals(1, accessService.generateAceResponses(acl).size());

        thrown.expect(KylinException.class);
        accessService.revokeWithSid(null, "MODELER", true);
    }

    @Test
    public void testExtPermissionInProject() {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        accessService.init(ae, AclPermission.ADMINISTRATION);

        Sid modeler = accessService.getSid("MODELER", true);
        Acl acl = accessService.grant(project, ae, AclPermission.ADMINISTRATION, modeler);

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
        acl = accessService.updateExtensionPermission(project, ae, accessRequest);
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
        acl = accessService.updateExtensionPermission(project, ae, accessRequestd);
        assertEquals(2, accessService.generateAceResponses(acl).size());

        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                assertNotEquals(CompositeAclPermission.class, ace.getPermission().getClass());
            }
        }

        // test get User Ext Permissions
        accessService.updateExtensionPermission(project, ae, accessRequest);
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
    public void testAclWithUnNaturalOrder() {
        val project = RandomUtil.randomUUIDStr();
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE,
                "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");

        // read from metadata
        MutableAclRecord acl = accessService.getAcl(ae);
        // order by sid_order in aceImpl
        // ADL6911(group), BDL6911(group), aCL6911(group), ACZ5815(user), ACZ5815(user), czw9976(user)
        List<AccessControlEntry> entries = acl.getEntries();

        checkEntries(entries);

        // grant
        acl = accessService.grant(project, ae, BasePermission.ADMINISTRATION, accessService.getSid("atest1", true));

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
        acl = accessService.revoke(project, ae, 6);
        Assert.assertThrows(KylinException.class, () -> accessService.revoke(project, null, 6));

        entries = acl.getEntries();
        checkAcl(entries);
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

    @Test
    public void testAclWithUnNaturalOrderUpdate() throws IOException {
        val project = RandomUtil.randomUUIDStr();
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
        acl = accessService.grant(project, ae, BasePermission.ADMINISTRATION, accessService.getSid("atest1", true));

        entries = acl.getEntries();

        // update atest1
        assertEquals(BasePermission.ADMINISTRATION, entries.get(5).getPermission());

        acl = accessService.update(project, ae, 5, BasePermission.READ);
        entries = acl.getEntries();
        assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        assertEquals("atest1", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());
        assertEquals(BasePermission.READ, entries.get(5).getPermission());

        acl = accessService.update(project, ae, 1,
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

        Assert.assertThrows(KylinException.class,
                () -> accessService.update(project, null, 5, AclPermission.DATA_QUERY));
        Assert.assertThrows(KylinException.class, () -> accessService.update(project, ae, 5, null));
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
    public void testUpdateExtensionPermissionException() {
        val project = RandomUtil.randomUUIDStr();
        Assert.assertThrows(MsgPicker.getMsg().getAclDomainNotFound(), KylinException.class,
                () -> accessService.updateExtensionPermission(project, null, null));
        UserAclManager.getInstance(getTestConfig()).deletePermission("admin", AclPermission.DATA_QUERY);
        Mockito.when(userService.listSuperAdminUsers()).thenReturn(Collections.emptyList());
        ThrowingRunnable func = () -> accessService.updateExtensionPermission(project, new AclRecord(), null);
        Assert.assertThrows(MsgPicker.getMsg().getAclPermissionRequired(), KylinException.class, func);
    }
}
