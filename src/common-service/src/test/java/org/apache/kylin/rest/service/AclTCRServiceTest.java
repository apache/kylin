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

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;
import static org.springframework.security.acls.domain.BasePermission.ADMINISTRATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.acl.SensitiveDataMask;
import org.apache.kylin.metadata.acl.SensitiveDataMaskInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.request.AclTCRRequest;
import org.apache.kylin.rest.response.AclTCRResponse;
import org.apache.kylin.rest.response.SidPermissionWithAclResponse;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
public class AclTCRServiceTest extends NLocalFileMetadataTestCase {

    private final String user1 = "u1";
    private final String user2 = "u2";
    private final String user3 = "u3";
    private final String user4 = "u4";
    private final String user5 = "u5";
    private final String user6 = "u6";
    private final String group1 = "g1";
    private final String group2 = "g2";
    private final String group3 = "g3";

    private final String allAuthorizedUser1 = "a1u1";
    private final String allAuthorizedGroup1 = "a1g1";

    private final String projectDefault = "default";

    private final String dbTblUnload = "db.tbl_unload";

    private final String revokeUser = "revoke_user";
    private final String revokeGroup = "revoke_group";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @Mock
    private UserAclService userAclService = Mockito.spy(UserAclService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    private UserService userService = Mockito.spy(KylinUserService.class);

    @Mock
    private IUserGroupService userGroupService = Mockito.spy(IUserGroupService.class);

    @Mock
    private AclService aclService = Mockito.spy(AclService.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Before
    public void setUp() throws IOException {
        PowerMockito.mockStatic(SpringContext.class);

        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata("src/test/resources/ut_acl");
        ReflectionTestUtils.setField(userAclService, "userService", userService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(aclEvaluate, "userAclService", userAclService);
        ReflectionTestUtils.setField(aclTCRService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclTCRService, "accessService", accessService);
        ReflectionTestUtils.setField(aclTCRService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(userService, "userAclService", userAclService);
        ReflectionTestUtils.setField(aclTCRService, "userService", userService);
        ReflectionTestUtils.setField(accessService, "userService", userService);
        ReflectionTestUtils.setField(accessService, "aclService", aclService);
        ReflectionTestUtils.setField(aclTCRService, "projectService", projectService);
        ReflectionTestUtils.setField(accessService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(accessService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(accessService, "userAclService", userAclService);
        initUsers();

        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    private void initUsers() throws IOException {
        NKylinUserManager userManager = NKylinUserManager.getInstance(getTestConfig());
        userManager.update(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
        userManager.update(new ManagedUser("ANALYST", "ANALYST", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ANALYST))));
        userManager.update(new ManagedUser(user1, "Q`w11g23", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS))));
        userManager.update(new ManagedUser(user2, "Q`w11g23", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ANALYST))));
        userManager.update(new ManagedUser(user3, "Q`w11g23", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
        userManager.update(new ManagedUser(user4, "Q`w11g23", false, Arrays.asList(//
                new SimpleGrantedAuthority(ROLE_ADMIN))));
        userManager.update(new ManagedUser(user5, "Q`w11g23", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS))));
        userManager.update(new ManagedUser(user6, "Q`w11g23", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS), new SimpleGrantedAuthority("g3"))));

        switchToAdmin();
        // mock AclManager bean in spring
        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenReturn(applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenReturn(PowerMockito.mock(PermissionFactory.class));
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenReturn(PowerMockito.mock(PermissionGrantingStrategy.class));

        AclManager aclManger = AclManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(projectDefault);
        AclEntity projectAE = AclEntityFactory.createAclEntity(AclEntityType.PROJECT_INSTANCE,
                projectInstance.getUuid());
        AclServiceTest.MockAclEntity userAE = new AclServiceTest.MockAclEntity(user5);
        MutableAclRecord projectAcl = (MutableAclRecord) aclService.createAcl(new ObjectIdentityImpl(projectAE));
        aclService.createAcl(new ObjectIdentityImpl(userAE));
        Sid sidUser5 = accessService.getSid(user5, true);
        Sid sidGroup3 = accessService.getSid(group3, false);

        Map<Sid, Permission> map = new HashMap<>();
        map.put(sidUser5, ADMINISTRATION);
        map.put(sidGroup3, ADMINISTRATION);
        aclManger.batchUpsertAce(projectAcl, map);
        //        aclManger.upsertAce(projectAcl, sidGroup3, ADMINISTRATION);
    }

    private void switchToAdmin() {
        Authentication adminAuth = new TestingAuthenticationToken("ADMIN", "ADMIN", "ROLE_ADMIN");
        SecurityContextHolder.getContext().setAuthentication(adminAuth);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGrantProjectPermission() {
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        final String uuid = aclTCRService.getManager(NProjectManager.class).getProject(projectDefault).getUuid();

        List<AccessRequest> ars = Lists.newArrayList();
        AccessRequest u1ar = new AccessRequest();
        u1ar.setSid(user1);
        u1ar.setPrincipal(true);
        ars.add(u1ar);

        AccessRequest g1ar = new AccessRequest();
        g1ar.setSid(group1);
        g1ar.setPrincipal(false);
        ars.add(g1ar);
        aclTCRService.updateAclTCR(uuid, ars);

        Set<String> tables = manager.getAuthorizedTables(user1, Sets.newHashSet(group1));
        Assert.assertTrue(tables.contains("DEFAULT.TEST_ORDER"));
        Assert.assertTrue(tables.contains("DEFAULT.TEST_COUNTRY"));

        getTestConfig().setProperty("kylin.acl.project-internal-default-permission-granted", "false");
        ars = Lists.newArrayList();
        AccessRequest u2ar = new AccessRequest();
        u1ar.setSid(user2);
        u1ar.setPrincipal(true);
        ars.add(u2ar);

        AccessRequest g2ar = new AccessRequest();
        g2ar.setSid(group2);
        g2ar.setPrincipal(false);
        ars.add(g2ar);
        aclTCRService.updateAclTCR(uuid, ars);

        tables = manager.getAuthorizedTables(user2, Sets.newHashSet(group2));
        Assert.assertFalse(tables.contains("DEFAULT.TEST_ORDER"));
        Assert.assertFalse(tables.contains("DEFAULT.TEST_COUNTRY"));
    }

    private List<AclTCRRequest.Row> getAclTCRRequestRow(AclTCRRequest acl, String database, String table) {
        List<AclTCRRequest.Row> result = new ArrayList<>();
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table) && tb.getRows() != null) {
                    result = tb.getRows();
                }
            }
        }
        return result;
    }

    private boolean getTableAuthorized(AclTCRRequest acl, String database, String table) {
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table)) {
                    return tb.isAuthorized();
                }
            }
        }
        return false;
    }

    private boolean getColumnAuthorized(AclTCRRequest acl, String database, String table, String column) {
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table)) {
                    if (!tb.isAuthorized())
                        return false;
                    for (val cn : tb.getColumns()) {
                        if (cn.getColumnName().equals(column))
                            return cn.isAuthorized();
                    }
                }
            }
        }
        return false;
    }

    private SensitiveDataMask.MaskType getColumnDataMask(AclTCRRequest acl, String database, String table,
            String column) {
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table)) {
                    if (!tb.isAuthorized())
                        return null;
                    for (val cn : tb.getColumns()) {
                        if (cn.getColumnName().equals(column))
                            return cn.getDataMaskType();
                    }
                }
            }
        }
        return null;
    }

    private List<AclTCRRequest> fillAclTCRRequest(AclTCRRequest origin) {
        val allTables = NTableMetadataManager.getInstance(getTestConfig(), projectDefault).listAllTables();
        Map<String, AclTCRRequest> requests = new HashMap<>();
        requests.put("DEFAULT", new AclTCRRequest());
        requests.put("EDW", new AclTCRRequest());
        requests.put("SSB", new AclTCRRequest());
        allTables.forEach(table -> {
            String database = table.getDatabase();
            val acl = requests.get(database);
            acl.setDatabaseName(database);
            AclTCRRequest.Table tb = new AclTCRRequest.Table();
            tb.setTableName(table.getName());
            tb.setRows(getAclTCRRequestRow(origin, database, table.getName()));
            tb.setAuthorized(getTableAuthorized(origin, database, table.getName()));
            List<AclTCRRequest.Column> columns = new ArrayList<>();
            Arrays.stream(table.getColumns()).forEach(columnDesc -> {
                AclTCRRequest.Column column = new AclTCRRequest.Column();
                column.setAuthorized(getColumnAuthorized(origin, database, table.getName(), columnDesc.getName()));
                column.setDataMaskType(getColumnDataMask(origin, database, table.getName(), columnDesc.getName()));
                column.setColumnName(columnDesc.getName());
                columns.add(column);
            });
            tb.setColumns(columns);
            List<AclTCRRequest.Table> tbs = new ArrayList<>();
            if (acl.getTables() != null)
                tbs.addAll(acl.getTables());
            tbs.add(tb);
            acl.setTables(tbs);
        });

        List<AclTCRRequest> res = new ArrayList<>();
        res.add(requests.get("DEFAULT"));
        res.add(requests.get("EDW"));
        res.add(requests.get("SSB"));
        return res;
    }

    @Test
    public void testUpdateAclTCRRequest() throws IOException {
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        final String uuid = aclTCRService.getManager(NProjectManager.class).getProject(projectDefault).getUuid();

        List<AccessRequest> ars = Lists.newArrayList();
        AccessRequest u1ar = new AccessRequest();
        u1ar.setSid(user1);
        u1ar.setPrincipal(true);
        u1ar.setPermission("ADMINISTRATION");
        ars.add(u1ar);
        getTestConfig().setProperty("kylin.acl.project-internal-default-permission-granted", "false");
        aclTCRService.updateAclTCR(uuid, ars);

        AccessRequest g1ar = new AccessRequest();
        g1ar.setSid(group1);
        g1ar.setPrincipal(false);
        ars.add(g1ar);
        aclTCRService.updateAclTCR(uuid, ars);
        Set<String> tables = manager.getAuthorizedTables(user1, Sets.newHashSet(group1));
        Assert.assertTrue(tables.contains("DEFAULT.TEST_COUNTRY"));

        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        AclTCRRequest.Table u1t2 = new AclTCRRequest.Table();
        u1t2.setTableName("TEST_ACCOUNT");
        u1t2.setAuthorized(false);

        AclTCRRequest.Column u1c1 = new AclTCRRequest.Column();
        u1c1.setColumnName("ORDER_ID");
        u1c1.setAuthorized(true);
        AclTCRRequest.Column u1c2 = new AclTCRRequest.Column();
        u1c2.setColumnName("BUYER_ID");
        u1c2.setAuthorized(false);
        AclTCRRequest.Column u1c3 = new AclTCRRequest.Column();
        u1c3.setColumnName("TEST_DATE_ENC");
        u1c3.setAuthorized(true);
        u1c3.setDataMaskType(SensitiveDataMask.MaskType.DEFAULT);
        // add columns
        u1t1.setColumns(Arrays.asList(u1c1, u1c2, u1c3));

        AclTCRRequest.Row u1r1 = new AclTCRRequest.Row();
        u1r1.setColumnName("ORDER_ID");
        u1r1.setItems(Arrays.asList("100100", "100101", "100102"));
        //add rows
        u1t1.setRows(Arrays.asList(u1r1));

        //add tables
        request.setTables(Arrays.asList(u1t1, u1t2));

        // test update AclTCR
        aclTCRService.updateAclTCR(projectDefault, user1, true, fillAclTCRRequest(request));
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertFalse(tables.contains("DEFAULT.TEST_COUNTRY"));
        SensitiveDataMaskInfo maskInfo = manager.getSensitiveDataMaskInfo(user1, null);
        Assert.assertNotNull(maskInfo.getMask("DEFAULT", "TEST_ORDER", "TEST_DATE_ENC"));

        // test revoke AclTCR
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertTrue(tables.contains("DEFAULT.TEST_ORDER"));
        aclTCRService.revokeAclTCR(uuid, user1, true);
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertFalse(tables.contains("DEFAULT.TEST_ORDER"));

        tables = manager.getAuthorizedTables(null, Sets.newHashSet(group1));
        Assert.assertFalse(tables.contains("DEFAULT.TEST_COUNTRY"));

        Mockito.when(projectService.getOwnedProjects()).thenReturn(Lists.newArrayList("default"));
        aclTCRService.revokeAclTCR(group1, false);
        tables = manager.getAuthorizedTables(null, Sets.newHashSet(group1));
        Assert.assertFalse(tables.contains("DEFAULT.TEST_COUNTRY"));

        // test unload table
        aclTCRService.updateAclTCR(projectDefault, user1, true, fillAclTCRRequest(request));
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertTrue(tables.contains("DEFAULT.TEST_ORDER"));
        aclTCRService.unloadTable(projectDefault, "DEFAULT.TEST_ORDER");
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertFalse(tables.contains("DEFAULT.TEST_ORDER"));
        assertKylinExeption(() -> {
            aclTCRService.updateAclTCR(projectDefault, user4, true, fillAclTCRRequest(request));
        }, "Admin is not supported to update permission.");
        assertKylinExeption(() -> {
            aclTCRService.updateAclTCR(projectDefault, user5, true, fillAclTCRRequest(request));
        }, "Admin is not supported to update permission.");
    }

    @Test
    public void testInvalidAclTCRRequest() {
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        AclTCRRequest.Column u1c1 = new AclTCRRequest.Column();
        u1c1.setColumnName("ORDER_ID");
        u1c1.setAuthorized(true);
        // add columns
        u1t1.setColumns(Arrays.asList(u1c1));

        AclTCRRequest.Row u1r1 = new AclTCRRequest.Row();
        u1r1.setColumnName("TEST_EXTENDED_COLUMN");
        u1r1.setItems(Arrays.asList("abc"));

        AclTCRRequest.Row u1r2 = new AclTCRRequest.Row();
        u1r2.setColumnName("ORDER_ID");
        u1r2.setItems(Arrays.asList("bbb"));

        //add rows
        u1t1.setRows(Arrays.asList(u1r1, u1r2));

        AclTCRRequest.Table u1t2 = new AclTCRRequest.Table();
        u1t2.setTableName("TEST_ACCOUNT");
        u1t2.setAuthorized(false);
        //add tables
        request.setTables(Arrays.asList(u1t2, u1t1));
        try {
            aclTCRService.updateAclTCR(projectDefault, user1, true, fillAclTCRRequest(request));
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e.getCause() instanceof KylinException);
            Assert.assertTrue(e.getCause().getMessage()
                    .contains("Can’t assign value(s) for the column \"DEFAULT.TEST_ORDER.ORDER_ID\""));
        }
    }

    @Test
    public void testGetAclTCRResponse() throws IOException {
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);
        Assert.assertTrue(aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).stream()
                .anyMatch(resp -> resp.getTables().stream().anyMatch(t -> "TEST_ORDER".equals(t.getTableName()))));

        AclTCR aclTCR = new AclTCR();
        AclTCR.Table table = new AclTCR.Table();
        AclTCR.ColumnRow columnRow1 = new AclTCR.ColumnRow();
        AclTCR.Column column1 = new AclTCR.Column();
        AclTCR.Row row1 = new AclTCR.Row();
        AclTCR.RealRow realRow1 = new AclTCR.RealRow();
        realRow1.addAll(Arrays.asList("100100", "100101", "100102"));
        row1.put("ORDER_ID", realRow1);
        row1.put("BUYER_ID", null);
        columnRow1.setRow(row1);
        column1.addAll(Arrays.asList("ORDER_ID", "BUYER_ID", "TEST_DATE_ENC"));
        columnRow1.setColumn(column1);
        columnRow1.setColumnSensitiveDataMask(
                Lists.newArrayList(new SensitiveDataMask("ORDER_ID", SensitiveDataMask.MaskType.AS_NULL)));
        table.put("DEFAULT.TEST_ORDER", columnRow1);
        table.put("DEFAULT.TEST_ACCOUNT", null);

        AclTCR.ColumnRow columnRow2 = new AclTCR.ColumnRow();
        AclTCR.Row row2 = new AclTCR.Row();
        AclTCR.RealRow realRow2 = new AclTCR.RealRow();
        realRow2.addAll(Arrays.asList("country_a", "country_b"));
        row2.put("COUNTRY", realRow2);
        columnRow2.setRow(row2);
        table.put("DEFAULT.TEST_COUNTRY", columnRow2);

        aclTCR.setTable(table);

        manager.updateAclTCR(aclTCR, user1, true);

        List<AclTCRResponse> responses = aclTCRService.getAclTCRResponse(projectDefault, user1, true, true);

        Assert.assertTrue(responses.stream()
                .anyMatch(resp -> resp.getTables().stream().anyMatch(t -> "TEST_ORDER".equals(t.getTableName()))));
        Assert.assertTrue(responses.stream()
                .anyMatch(resp -> resp.getTables().stream().noneMatch(t -> "TEST_SITES".equals(t.getTableName()))));

        responses = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream()
                .anyMatch(t -> t.isAuthorized() && "TEST_ORDER".equals(t.getTableName()))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream()
                .anyMatch(t -> !t.isAuthorized() && "TEST_SITES".equals(t.getTableName()))));
        Assert.assertEquals(3, responses.stream().filter(resp -> "DEFAULT".equals(resp.getDatabaseName())).findAny()
                .get().getAuthorizedTableNum());
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(
                t -> t.getColumns().stream().anyMatch(c -> c.isAuthorized() && "ORDER_ID".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(
                t -> t.getColumns().stream().anyMatch(c -> c.isAuthorized() && "BUYER_ID".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(
                t -> t.getColumns().stream().anyMatch(c -> c.isAuthorized() && "COUNTRY".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(t -> t.getColumns()
                .stream().anyMatch(c -> !c.isAuthorized() && "TEST_TIME_ENC".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream()
                .anyMatch(resp -> resp.getTables().stream()
                        .anyMatch(t -> t.getRows().stream().anyMatch(r -> "COUNTRY".equals(r.getColumnName())
                                && "country_a,country_b".equals(String.join(",", r.getItems()))))));

        Assert.assertTrue(responses.stream()
                .anyMatch(resp -> resp.getTables().stream()
                        .anyMatch(t -> t.getColumns().stream().anyMatch(c -> "ORDER_ID".equals(c.getColumnName())
                                && c.getDataMaskType() == SensitiveDataMask.MaskType.AS_NULL))));

        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(t -> t.getColumns()
                .stream().anyMatch(c -> "BUYER_ID".equals(c.getColumnName()) && "bigint".equals(c.getDatatype())))));
    }

    @Test
    public void testGetAclTCRResponseWithAdmin() throws IOException {
        Mockito.doReturn(true).when(userService).isGlobalAdmin(user1);
        List<AclTCRResponse> responses = aclTCRService.getAclTCRResponse(projectDefault, user1, true, true);
        Assert.assertEquals(3, responses.size());
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream()
                .anyMatch(t -> t.isAuthorized() && "TEST_ORDER".equals(t.getTableName()))));
        Assert.assertTrue(responses.stream()
                .anyMatch(res -> "DEFAULT".equals(res.getDatabaseName()) && res.getTables().size() == 13));
        Assert.assertTrue(
                responses.stream().anyMatch(res -> "EDW".equals(res.getDatabaseName()) && res.getTables().size() == 3));
        Assert.assertTrue(
                responses.stream().anyMatch(res -> "SSB".equals(res.getDatabaseName()) && res.getTables().size() == 6));
    }

    @Test
    public void testGetAuthorizedTables() {
        String userName = "user1";
        Set<String> groups = Sets.newHashSet("group1");

        NKylinUserManager nKylinUserManager = Mockito.mock(NKylinUserManager.class);
        Mockito.doReturn(nKylinUserManager).when(aclTCRService).getKylinUserManager();

        Mockito.doReturn(groups).when(nKylinUserManager).getUserGroups(userName);
        Mockito.doReturn(null).when(aclTCRService).getAuthorizedTables("default", userName, Sets.newHashSet("group1"));

        Assert.assertNull(aclTCRService.getAuthorizedTables("default", userName));

        Mockito.reset(aclTCRService);
        AclTCRManager aclTCRManager = Mockito.mock(AclTCRManager.class);
        Mockito.doReturn(aclTCRManager).when(aclTCRService).getManager(AclTCRManager.class, "default");
        Mockito.doReturn(Lists.newArrayList()).when(aclTCRManager).getAclTCRs(userName, groups);

        NTableMetadataManager nTableMetadataManager = Mockito.mock(NTableMetadataManager.class);
        Mockito.doReturn(nTableMetadataManager).when(aclTCRService).getTableMetadataManager("default");
        Mockito.doReturn(Lists.newArrayList()).when(nTableMetadataManager).listAllTables();
        Mockito.doReturn(false).when(aclTCRService).canUseACLGreenChannel("default");
        Assert.assertEquals(0, aclTCRService.getAuthorizedTables("default", userName, groups).size());
    }

    private List<AclTCRRequest> getFillRequest() {
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        u1t1.setColumns(new ArrayList<>());
        u1t1.setRows(new ArrayList<>());
        request.setTables(Arrays.asList(u1t1));
        return fillAclTCRRequest(request);
    }

    @Test
    public void testACLTCRDuplicateDatabaseException() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Database [DEFAULT] is duplicated in API requests");
        val requests = getFillRequest();
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        requests.add(request);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDuplicateTableException() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Table [DEFAULT.TEST_ACCOUNT] is duplicated in API requests");
        val requests = getFillRequest();
        List<AclTCRRequest.Table> tables = new ArrayList<>(requests.get(0).getTables());
        requests.get(0).getTables().stream().filter(x -> x.getTableName().equals("TEST_ACCOUNT")).forEach(tables::add);
        requests.get(0).setTables(tables);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDuplicateColumnException() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Column [DEFAULT.TEST_ACCOUNT.ACCOUNT_ID] is duplicated in API requests");
        val requests = getFillRequest();
        requests.get(0).getTables().forEach(table -> {
            if (table.getTableName().equals("TEST_ACCOUNT")) {
                List<AclTCRRequest.Column> columns = table.getColumns();
                AclTCRRequest.Column add = null;
                for (val column : columns) {
                    if (column.getColumnName().equals("ACCOUNT_ID")) {
                        add = column;
                    }
                }
                columns.add(add);
            }
        });
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyDatabaseName() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘database_name’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).setDatabaseName(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyTables() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getEmptyTableList());
        val requests = getFillRequest();
        requests.get(0).setTables(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDatabaseMiss() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("All the databases should be defined and the database below are missing: (DEFAULT)");
        val requests = getFillRequest();
        requests.remove(0);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyTableName() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘table_name’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).getTables().get(0).setTableName(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRTableNotExist() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t find table \"DEFAULT.NOTEXIST\". Please check and try again.");
        val requests = getFillRequest();
        List<AclTCRRequest.Table> tables = new ArrayList<>(requests.get(0).getTables());
        AclTCRRequest.Table table = new AclTCRRequest.Table();
        table.setTableName("notexist");
        table.setRows(new ArrayList<>());
        requests.get(0).getTables().stream().filter(x -> x.getTableName().equals("TEST_ACCOUNT"))
                .forEach(x -> table.setColumns(x.getColumns()));
        tables.add(table);
        requests.get(0).setTables(tables);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRColumnNotExist() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Column:[DEFAULT.TEST_ACCOUNT.NOTEXIST] is not exist");
        val requests = getFillRequest();
        requests.get(0).getTables().forEach(table -> {
            if (table.getTableName().equals("TEST_ACCOUNT")) {
                List<AclTCRRequest.Column> columns = table.getColumns();
                AclTCRRequest.Column add = new AclTCRRequest.Column();
                add.setColumnName("notexist");
                columns.add(add);
            }
        });
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDatabaseNotExist() throws IOException {
        val requests = getFillRequest();
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("notexist");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        val any = requests.stream().filter(
                aclTCRRequest -> aclTCRRequest.getTables().stream().noneMatch(table -> table.getColumns().isEmpty()))
                .findAny();
        Assert.assertTrue(any.isPresent());
        u1t1.setColumns(Collections.singletonList(any.get().getTables().get(0).getColumns().get(0)));
        u1t1.setRows(new ArrayList<>());
        request.setTables(Collections.singletonList(u1t1));
        requests.add(request);
        Assert.assertThrows("Can’t find database \"NOTEXIST\". Please check and try again.", KylinException.class,
                () -> aclTCRService.updateAclTCR(projectDefault, user1, true, requests));
    }

    @Test
    public void testACLTCREmptyColumns() throws IOException {
        val requests = getFillRequest();
        val any = requests.stream().filter(
                aclTCRRequest -> aclTCRRequest.getTables().stream().noneMatch(table -> table.getColumns().isEmpty()))
                .findAny();
        Assert.assertTrue(any.isPresent());
        any.get().getTables().get(0).setColumns(null);
        Assert.assertThrows("Invalid value for parameter ‘columns’ which should not be empty", KylinException.class,
                () -> aclTCRService.updateAclTCR(projectDefault, user1, true, requests));
    }

    @Test
    public void testACLTCREmptyRows() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘rows’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).getTables().get(0).setRows(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyColumnName() throws IOException {
        val requests = getFillRequest();
        val any = requests.stream().filter(
                aclTCRRequest -> aclTCRRequest.getTables().stream().noneMatch(table -> table.getColumns().isEmpty()))
                .findAny();
        Assert.assertTrue(any.isPresent());
        any.get().getTables().get(0).getColumns().get(0).setColumnName(null);
        Assert.assertThrows("Invalid value for parameter ‘column_name’ which should not be empty", KylinException.class,
                () -> aclTCRService.updateAclTCR(projectDefault, user1, true, requests));
    }

    @Test
    public void testACLTCRTableMiss() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "All the tables should be defined and the table below are missing: (DEFAULT.TEST_ACCOUNT)");
        val requests = getFillRequest();
        List<AclTCRRequest.Table> tables = new ArrayList<>(requests.get(0).getTables());
        requests.get(0).getTables().stream().filter(x -> x.getTableName().equals("TEST_ACCOUNT"))
                .forEach(tables::remove);
        requests.get(0).setTables(tables);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRColumnMiss() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "All the columns should be defined and the column below are missing: (DEFAULT.TEST_ACCOUNT.ACCOUNT_ID)");
        val requests = getFillRequest();
        requests.get(0).getTables().forEach(table -> {
            if (table.getTableName().equals("TEST_ACCOUNT")) {
                List<AclTCRRequest.Column> columns = table.getColumns();
                AclTCRRequest.Column add = null;
                for (val column : columns) {
                    if (column.getColumnName().equals("ACCOUNT_ID")) {
                        add = column;
                    }
                }
                columns.remove(add);
            }
        });
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testMergeACLTCRWithRevokeGrantColumns() throws IOException {
        // grant acl tcr
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        List<AclTCRResponse> response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        AtomicInteger totalTableNum = new AtomicInteger();
        AtomicInteger authorizedTableNum = new AtomicInteger();

        AtomicInteger totalColumnNum = new AtomicInteger();
        AtomicInteger authorizedColumnNum = new AtomicInteger();

        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM")).anyMatch(table -> {
                            totalTableNum.set(aclTCRResponse.getTotalTableNum());
                            authorizedTableNum.set(aclTCRResponse.getAuthorizedTableNum());
                            totalColumnNum.set(table.getTotalColumnNum());
                            authorizedColumnNum.set(table.getAuthorizedColumnNum());
                            return table.getColumns().stream().anyMatch(
                                    column -> column.isAuthorized() && column.getColumnName().equals("DIM_CRE_USER"));
                        })));

        Assert.assertTrue(totalTableNum.get() > 0);
        Assert.assertTrue(authorizedTableNum.get() > 0);
        Assert.assertTrue(totalColumnNum.get() > 0);
        Assert.assertTrue(authorizedColumnNum.get() > 0);

        // revoke column
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("EDW");
        AclTCRRequest.Table tableRequest = new AclTCRRequest.Table();
        tableRequest.setTableName("TEST_SELLER_TYPE_DIM");
        tableRequest.setAuthorized(true);

        AclTCRRequest.Column columnRequest = new AclTCRRequest.Column();
        columnRequest.setColumnName("DIM_CRE_USER");
        columnRequest.setAuthorized(false);
        tableRequest.setColumns(Collections.singletonList(columnRequest));
        request.setTables(Collections.singletonList(tableRequest));

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));

        response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM"))
                        .anyMatch(table -> table.getAuthorizedColumnNum() == authorizedColumnNum.get() - 1
                                && table.getColumns().stream().anyMatch(column -> !column.isAuthorized()
                                        && column.getColumnName().equals("DIM_CRE_USER")))));

        // grant columnRequest
        columnRequest.setColumnName("DIM_CRE_USER");
        columnRequest.setAuthorized(true);

        tableRequest.setColumns(Collections.singletonList(columnRequest));
        request.setTables(Collections.singletonList(tableRequest));

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));
        response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);
        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM"))
                        .anyMatch(table -> table.getAuthorizedColumnNum() == authorizedColumnNum.get()
                                && table.getColumns().stream().anyMatch(column -> column.isAuthorized()
                                        && column.getColumnName().equals("DIM_CRE_USER")))));
    }

    @Test
    public void testMergeACLTCRWithRevokeGrantTable() throws IOException {
        // grant acl tcr
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        AtomicInteger totalTableNum = new AtomicInteger();
        AtomicInteger authorizedTableNum = new AtomicInteger();

        AtomicInteger totalColumnNum = new AtomicInteger();
        AtomicInteger authorizedColumnNum = new AtomicInteger();

        var response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM")).anyMatch(table -> {
                            totalTableNum.set(aclTCRResponse.getTotalTableNum());
                            authorizedTableNum.set(aclTCRResponse.getAuthorizedTableNum());
                            totalColumnNum.set(table.getTotalColumnNum());
                            authorizedColumnNum.set(table.getAuthorizedColumnNum());
                            return table.getColumns().stream().anyMatch(
                                    column -> column.isAuthorized() && column.getColumnName().equals("DIM_CRE_USER"));
                        })));

        // revoke tableRequest
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("EDW");
        AclTCRRequest.Table tableRequest = new AclTCRRequest.Table();
        tableRequest.setTableName("TEST_SELLER_TYPE_DIM");
        tableRequest.setAuthorized(false);

        request.setTables(Collections.singletonList(tableRequest));

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));

        response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);
        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM") && !table.isAuthorized())
                        .anyMatch(table -> table.getAuthorizedColumnNum() == 0
                                && table.getColumns().stream().noneMatch(AclTCRResponse.Column::isAuthorized))));

        // grant tableRequest
        tableRequest.setAuthorized(true);

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));

        response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);
        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM") && table.isAuthorized())
                        .anyMatch(table -> table.getAuthorizedColumnNum() == authorizedColumnNum.get()
                                && table.getColumns().stream().allMatch(AclTCRResponse.Column::isAuthorized))));
    }

    @Test
    public void testMergeACLTCRWithBatchUpdateRowAcl() throws IOException {
        // grant acl tcr
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        // revoke columnRequest
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("EDW");
        AclTCRRequest.Table tableRequest = new AclTCRRequest.Table();
        tableRequest.setAuthorized(true);
        tableRequest.setTableName("TEST_SELLER_TYPE_DIM");

        AclTCRRequest.Row row = new AclTCRRequest.Row();
        tableRequest.setRows(new ArrayList<>());
        row.setColumnName("DIM_CRE_USER");
        row.setItems(Arrays.asList("user1", "user2"));
        tableRequest.getRows().add(row);

        row = new AclTCRRequest.Row();
        row.setColumnName("DIM_CRE_DATE");
        row.setItems(Arrays.asList("2020-01-01 00:00:00", "2020-01-02 00:00:00"));
        tableRequest.getRows().add(row);
        request.setTables(Collections.singletonList(tableRequest));

        AclTCRRequest.Column column = new AclTCRRequest.Column();
        column.setAuthorized(true);
        column.setColumnName("NOT_EXIST_COLUMN");
        tableRequest.setColumns(Lists.newArrayList(column));

        assertKylinExeption(
                () -> aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request)),
                "Column:[EDW.TEST_SELLER_TYPE_DIM.NOT_EXIST_COLUMN] is not exist.");

        column.setColumnName("DIM_CRE_DATE");

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));

        var response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM"))
                        .anyMatch(table -> table.getColumns().stream().allMatch(AclTCRResponse.Column::isAuthorized)
                                && table.getRows().size() == 2)));

        tableRequest.setRows(new ArrayList<>());
        row.setColumnName("DIM_CRE_USER");
        row.setItems(Arrays.asList("user1", "user2"));
        tableRequest.getRows().add(row);

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));

        response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM"))
                        .anyMatch(table -> table.getColumns().stream().allMatch(AclTCRResponse.Column::isAuthorized)
                                && table.getRows().size() == 1)));

        tableRequest.setRows(new ArrayList<>());
        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));

        response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(response.stream().filter(aclTCRResponse -> aclTCRResponse.getDatabaseName().equals("EDW"))
                .anyMatch(aclTCRResponse -> aclTCRResponse.getTables().stream()
                        .filter(table -> table.getTableName().equals("TEST_SELLER_TYPE_DIM"))
                        .anyMatch(table -> table.getColumns().stream().allMatch(AclTCRResponse.Column::isAuthorized)
                                && table.getRows().size() == 0)));

    }

    @Test
    public void testMergeACLTCRWithGrantRowAclWithUnauthorizedColumn() throws IOException {
        // grant acl tcr
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        // revoke column
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("EDW");
        AclTCRRequest.Table tableRequest = new AclTCRRequest.Table();
        tableRequest.setTableName("TEST_SELLER_TYPE_DIM");
        tableRequest.setAuthorized(true);

        AclTCRRequest.Column columnRequest = new AclTCRRequest.Column();
        columnRequest.setColumnName("DIM_CRE_USER");
        columnRequest.setAuthorized(false);
        tableRequest.setColumns(Collections.singletonList(columnRequest));
        request.setTables(Collections.singletonList(tableRequest));

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));

        // grant acl
        AclTCRRequest.Row row = new AclTCRRequest.Row();
        tableRequest.setRows(new ArrayList<>());
        row.setColumnName("DIM_CRE_USER");
        row.setItems(Arrays.asList("user1", "user2"));
        tableRequest.getRows().add(row);

        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof KylinException) {
                    return ((KylinException) item).getMessage()
                            .contains("doesn’t have access to the column \"DIM_CRE_USER\"");
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));
    }

    @Test
    public void testMergeACLTCRWithDependencyColumnDependsOnDependencyColumn() throws IOException {
        // grant acl tcr
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        // revoke column
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("EDW");
        AclTCRRequest.Table tableRequest = new AclTCRRequest.Table();
        tableRequest.setTableName("TEST_SELLER_TYPE_DIM");
        tableRequest.setAuthorized(true);
        tableRequest.setColumns(new ArrayList<>());

        AclTCRRequest.Column columnRequest = new AclTCRRequest.Column();
        columnRequest.setDependentColumns(new ArrayList<>());
        columnRequest.setColumnName("DIM_CRE_USER");
        columnRequest.setAuthorized(true);
        AclTCRRequest.DependentColumnData dependentColumnData = new AclTCRRequest.DependentColumnData();
        dependentColumnData.setColumnIdentity("EDW.TEST_SELLER_TYPE_DIM.DIM_CRE_DATE");
        dependentColumnData.setValues(new String[] { "2020-01-01 00:00:00", "2020-01-02 00:00:00" });

        columnRequest.getDependentColumns().add(dependentColumnData);
        tableRequest.getColumns().add(columnRequest);

        columnRequest = new AclTCRRequest.Column();
        columnRequest.setDependentColumns(new ArrayList<>());
        columnRequest.setColumnName("DIM_CRE_DATE");
        columnRequest.setAuthorized(true);
        dependentColumnData = new AclTCRRequest.DependentColumnData();
        dependentColumnData.setColumnIdentity("EDW.TEST_SELLER_TYPE_DIM.DIM_CRE_USER");
        dependentColumnData.setValues(new String[] { "user1", "user2" });

        columnRequest.getDependentColumns().add(dependentColumnData);

        tableRequest.getColumns().add(columnRequest);
        request.setTables(Collections.singletonList(tableRequest));

        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof KylinException) {
                    return ((KylinException) item).getMessage()
                            .contains("Can’t set association rules on the column \"DIM_CRE_DATE, DIM_CRE_USER\"");
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {

            }
        });

        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));
    }

    @Test
    public void testMergeACLTCRWithUnsupportedMaskDatatype() throws IOException {
        // Boolean, Map, and Array data types do not support data masking.
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getManager(AclTCRManager.class, projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);

        //  column
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table tableRequest = new AclTCRRequest.Table();
        tableRequest.setTableName("TEST_MEASURE");
        tableRequest.setAuthorized(true);

        AclTCRRequest.Column columnRequest = new AclTCRRequest.Column();
        columnRequest.setColumnName("FLAG");
        columnRequest.setAuthorized(true);
        columnRequest.setDataMaskType(SensitiveDataMask.MaskType.AS_NULL);
        tableRequest.setColumns(Collections.singletonList(columnRequest));
        request.setTables(Collections.singletonList(tableRequest));

        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof KylinException) {
                    return ((KylinException) item).getMessage().contains("boolean, map or array");
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        aclTCRService.mergeAclTCR(projectDefault, user1, true, Collections.singletonList(request));
    }

    @Test
    public void testACLTCRInvalidDataTypeLikeCondition() throws IOException {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRowAclNotStringType());
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        u1t1.setColumns(new ArrayList<>());
        u1t1.setRows(new ArrayList<>());
        AclTCRRequest.Row u1r1 = new AclTCRRequest.Row();
        u1r1.setColumnName("ORDER_ID");
        u1r1.setItems(Arrays.asList("1%"));
        u1t1.setLikeRows(Lists.newArrayList(u1r1));
        request.setTables(Arrays.asList(u1t1));
        aclTCRService.mergeAclTCR(projectDefault, user1, true, Lists.newArrayList(request));
    }

    @Test
    public void testUpdateAclTCRWithEmptyColumn() throws IOException {
        // grant empty column table's acl
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("EMPTY_COLUMN");
        u1t1.setAuthorized(true);

        request.setTables(Collections.singletonList(u1t1));
        List<AclTCRRequest> aclTCRRequests = fillAclTCRRequest(request);
        Assert.assertTrue(aclTCRRequests.stream()
                .anyMatch(aclTCRRequest -> "DEFAULT".equals(aclTCRRequest.getDatabaseName())
                        && aclTCRRequest.getTables().stream().anyMatch(
                                table -> "EMPTY_COLUMN".equals(table.getTableName()) && table.getColumns().isEmpty())));
        aclTCRService.updateAclTCR(projectDefault, user1, true, aclTCRRequests);

        var response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(response.stream()
                .anyMatch(aclTCRResponse -> "DEFAULT".equals(aclTCRResponse.getDatabaseName())
                        && aclTCRResponse.getTables().stream().anyMatch(
                                table -> "EMPTY_COLUMN".equals(table.getTableName()) && table.isAuthorized())));

        // revoke empty column table's acl
        request.setDatabaseName("DEFAULT");
        u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("EMPTY_COLUMN");
        u1t1.setAuthorized(false);

        request.setTables(Collections.singletonList(u1t1));
        aclTCRRequests = fillAclTCRRequest(request);
        Assert.assertTrue(aclTCRRequests.stream()
                .anyMatch(aclTCRRequest -> "DEFAULT".equals(aclTCRRequest.getDatabaseName())
                        && aclTCRRequest.getTables().stream().anyMatch(
                                table -> "EMPTY_COLUMN".equals(table.getTableName()) && table.getColumns().isEmpty())));
        aclTCRService.updateAclTCR(projectDefault, user1, true, aclTCRRequests);

        response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(response.stream()
                .anyMatch(aclTCRResponse -> "DEFAULT".equals(aclTCRResponse.getDatabaseName())
                        && aclTCRResponse.getTables().stream().anyMatch(
                                table -> "EMPTY_COLUMN".equals(table.getTableName()) && !table.isAuthorized())));
    }

    @Test
    public void testMergeAndGetWithRowFilter() throws IOException {
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        val rf1 = new AclTCRRequest.RowFilter();
        val filterGroups = new ArrayList<AclTCRRequest.FilterGroup>();
        val fg1 = new AclTCRRequest.FilterGroup();
        fg1.setGroup(false);
        val filters = new ArrayList<AclTCRRequest.Filter>();
        val filter = new AclTCRRequest.Filter();
        filter.setColumnName("TEST_EXTENDED_COLUMN");
        filter.setInItems(Lists.newArrayList("a", "b"));
        filter.setLikeItems(Lists.newArrayList("1", "2"));
        filters.add(filter);
        fg1.setFilters(filters);
        filterGroups.add(fg1);
        rf1.setFilterGroups(filterGroups);
        u1t1.setRowFilter(rf1);
        request.setTables(Lists.newArrayList(u1t1));
        aclTCRService.mergeAclTCR(projectDefault, user1, true, Lists.newArrayList(request));
        List<AclTCRResponse> response = aclTCRService.getAclTCRResponse(projectDefault, user1, true, true);
        Assert.assertTrue(response.stream().anyMatch(aclTCRResponse -> {
            if (!"DEFAULT".equals(aclTCRResponse.getDatabaseName())) {
                return false;
            }

            return aclTCRResponse.getTables().stream().anyMatch(table -> {
                if (!"TEST_ORDER".equals(table.getTableName())) {
                    return false;
                }

                val f = table.getRowFilter().getFilterGroups().get(0).getFilters().get(0);
                return ("TEST_EXTENDED_COLUMN".equals(f.getColumnName()) && "a".equals(f.getInItems().get(0))
                        && "1".equals(f.getLikeItems().get(0)));
            });
        }));
    }

    @Test
    public void testUpdateAdminAndProjectAdminGroupTableAcl() {
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);

        AclTCRRequest.Column u1c1 = new AclTCRRequest.Column();
        u1c1.setColumnName("ORDER_ID");
        u1c1.setAuthorized(false);
        // add columns
        u1t1.setColumns(Collections.singletonList(u1c1));

        //add tables
        request.setTables(Collections.singletonList(u1t1));

        assertKylinExeption(() -> {
            aclTCRService.updateAclTCR(projectDefault, group3, false, fillAclTCRRequest(request));
        }, "Admin is not supported to update permission.");

        assertKylinExeption(() -> {
            aclTCRService.updateAclTCR(projectDefault, ROLE_ADMIN, false, fillAclTCRRequest(request));
        }, "Admin is not supported to update permission.");

        overwriteSystemProp("kylin.security.acl.admin-role", "ldap-admin");

        assertKylinExeption(() -> {
            aclTCRService.updateAclTCR(projectDefault, "ldap-admin", false, fillAclTCRRequest(request));
        }, "Admin is not supported to update permission.");
    }

    @Test
    public void testGetUserOrGroupAclPermissions() throws IOException {
        // test admin user
        List<String> projects = accessService.getGrantedProjectsOfUserOrGroup("ADMIN", true);
        Mockito.when(userService.isGlobalAdmin("ADMIN")).thenReturn(true);
        List<SidPermissionWithAclResponse> responses = accessService.getUserOrGroupAclPermissions(projects, "ADMIN",
                true);
        Assert.assertEquals(34, responses.size());
        Assert.assertTrue(responses.stream().allMatch(response -> "ADMIN".equals(response.getProjectPermission())));

        // test normal group
        addGroupAndGrantPermission("MANAGEMENT_GROUP", AclPermission.MANAGEMENT);
        Mockito.when(userGroupService.exists("MANAGEMENT_GROUP")).thenReturn(true);
        projects = accessService.getGrantedProjectsOfUserOrGroup("MANAGEMENT_GROUP", false);
        responses = accessService.getUserOrGroupAclPermissions(projects, "MANAGEMENT_GROUP", false);
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals("MANAGEMENT", responses.get(0).getProjectPermission());

        // add ANALYST user to a granted normal group
        addGroupAndGrantPermission("ROLE_ANALYST", AclPermission.OPERATION);
        Mockito.when(userGroupService.exists("ROLE_ANALYST")).thenReturn(true);
        userGroupService.modifyGroupUsers("ROLE_ANALYST", Lists.newArrayList("ANALYST"));
        responses = accessService.getUserOrGroupAclPermissions(projects, "ANALYST", true);
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals("OPERATION", responses.get(0).getProjectPermission());

        // test user have not project permission
        responses = accessService.getUserOrGroupAclPermissions(Lists.newArrayList("ssb"), "ANALYST", true);
        Assert.assertEquals(0, responses.size());
    }

    @Test
    public void testCheckRow() {
        Message msg = MsgPicker.getMsg();
        AclTCRRequest.Row row = new AclTCRRequest.Row();

        Assert.assertThrows(msg.getEmptyColumnName(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(AclTCRService.class, "checkRow", msg, row));
        row.setColumnName("column");
        Assert.assertThrows(msg.getEmptyItems(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(AclTCRService.class, "checkRow", msg, row));
    }

    private void addGroupAndGrantPermission(String group, Permission permission) throws IOException {
        ProjectInstance projectInstance = NProjectManager.getInstance(getTestConfig()).getProject("default");
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        userGroupService.addGroup(group);
        Sid sid = accessService.getSid(group, false);
        ReflectionTestUtils.invokeMethod(accessService, "grant", ae, permission, sid);
    }
}
