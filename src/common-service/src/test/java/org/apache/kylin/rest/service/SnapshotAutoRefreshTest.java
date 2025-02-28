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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CONFIG_NOT_SUPPORT_EDIT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.request.SnapshotConfigRequest;
import org.apache.kylin.rest.response.ProjectConfigResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

@MetadataInfo
class SnapshotAutoRefreshTest {
    @InjectMocks
    private ProjectService projectService = Mockito.spy(new ProjectService());
    @InjectMocks
    private final ProjectSmartServiceSupporter projectSmartService = Mockito.spy(ProjectSmartServiceSupporter.class);

    @BeforeEach
    void setup() {
        ReflectionTestUtils.setField(projectService, "projectSmartService", projectSmartService);
    }

    @Test
    void testCheckSnapshotAutoParamWithFail() {
        testCheckSnapshotAutoParamWithFail("");
        testCheckSnapshotAutoParamWithFail("abc");
        testCheckSnapshotAutoParamWithFail("1");
        testCheckSnapshotAutoParamWithFail("101");
    }

    private void testCheckSnapshotAutoParamWithFail(String value) {
        try {
            projectService.checkSnapshotAutoParam("test", value, 2, 100, "message");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            val expected = new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "test", "message");
            assertEquals(expected.getErrorCodeString(), ((KylinException) e).getErrorCodeString());
            assertEquals(expected.getLocalizedMessage(), e.getLocalizedMessage());
        }
    }

    @Test
    void testCheckSnapshotAutoParam() {
        val name = "test";
        val message = "message";
        projectService.checkSnapshotAutoParam(name, "2", 2, 100, message);
        projectService.checkSnapshotAutoParam(name, "100", 2, 100, message);
    }

    @Test
    void testAutoTriggerTimeFail() {
        testAutoTriggerTimeFail("snapshot_automatic_refresh_trigger_hours", "0 ~ 23", "24", null, null);
        testAutoTriggerTimeFail("snapshot_automatic_refresh_trigger_hours", "0 ~ 23", "-1", null, null);
        testAutoTriggerTimeFail("snapshot_automatic_refresh_trigger_minute", "0 ~ 59", "1", "60", null);
        testAutoTriggerTimeFail("snapshot_automatic_refresh_trigger_minute", "0 ~ 59", "1", "-1", null);
        testAutoTriggerTimeFail("snapshot_automatic_refresh_trigger_second", "0 ~ 59", "1", "0", "60");
        testAutoTriggerTimeFail("snapshot_automatic_refresh_trigger_second", "0 ~ 59", "1", "0", "-1");
    }

    private void testAutoTriggerTimeFail(String name, String message, String hours, String minute, String second) {
        try {
            val snapshotConfigRequest = new SnapshotConfigRequest();
            snapshotConfigRequest.setSnapshotAutoRefreshTriggerHours(hours);
            snapshotConfigRequest.setSnapshotAutoRefreshTriggerMinute(minute);
            snapshotConfigRequest.setSnapshotAutoRefreshTriggerSecond(second);
            projectService.checkSnapshotAutoTriggerTime(snapshotConfigRequest);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            var expected = new KylinException(PARAMETER_INVALID_SUPPORT_LIST, name, message);
            assertEquals(expected.getErrorCodeString(), ((KylinException) e).getErrorCodeString());
            assertEquals(expected.getLocalizedMessage(), e.getLocalizedMessage());
        }
    }

    @Test
    void testAutoTriggerTime() {
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerHours("0");
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerMinute("0");
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerSecond("0");
        projectService.checkSnapshotAutoTriggerTime(snapshotConfigRequest);
    }

    @Test
    void testAutoRefreshConfigDefaultRefreshEnabled() {
        var snapshotConfigRequest = new SnapshotConfigRequest();
        projectService.checkSnapshotAutoRefreshConfig(snapshotConfigRequest);
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        projectService.checkSnapshotAutoRefreshConfig(snapshotConfigRequest);
    }

    @Test
    void testAutoRefreshConfigTimeModeFail() {
        testAutoRefreshConfigTimeModeFail(null, null, "snapshot_automatic_refresh_time_mode", "DAY, HOURS, MINUTE",
                null, null, null);
        testAutoRefreshConfigTimeModeFail("123", null, "snapshot_automatic_refresh_time_mode", "DAY, HOURS, MINUTE",
                null, null, null);

        testAutoRefreshConfigTimeModeFail("MINUTE", null, "snapshot_automatic_refresh_time_interval", "1 ~ 59", null,
                null, null);
        testAutoRefreshConfigTimeModeFail("MINUTE", "0", "snapshot_automatic_refresh_time_interval", "1 ~ 59", null,
                null, null);
        testAutoRefreshConfigTimeModeFail("MINUTE", "60", "snapshot_automatic_refresh_time_interval", "1 ~ 59", null,
                null, null);

        testAutoRefreshConfigTimeModeFail("HOURS", null, "snapshot_automatic_refresh_time_interval", "1 ~ 23", null,
                null, null);
        testAutoRefreshConfigTimeModeFail("HOURS", "0", "snapshot_automatic_refresh_time_interval", "1 ~ 23", null,
                null, null);
        testAutoRefreshConfigTimeModeFail("HOURS", "60", "snapshot_automatic_refresh_time_interval", "1 ~ 23", null,
                null, null);

        testAutoRefreshConfigTimeModeFail("DAY", null, "snapshot_automatic_refresh_time_interval", "> 1", null, null,
                null);
        testAutoRefreshConfigTimeModeFail("DAY", "0", "snapshot_automatic_refresh_time_interval", "> 1", null, null,
                null);

        testAutoRefreshConfigTimeModeFail("DAY", "1", "snapshot_automatic_refresh_trigger_hours", "0 ~ 23", "24", null,
                null);
        testAutoRefreshConfigTimeModeFail("DAY", "1", "snapshot_automatic_refresh_trigger_hours", "0 ~ 23", "-1", null,
                null);

        testAutoRefreshConfigTimeModeFail("DAY", "1", "snapshot_automatic_refresh_trigger_minute", "0 ~ 59", "1", "60",
                null);
        testAutoRefreshConfigTimeModeFail("DAY", "1", "snapshot_automatic_refresh_trigger_minute", "0 ~ 59", "1", "-1",
                null);

        testAutoRefreshConfigTimeModeFail("DAY", "1", "snapshot_automatic_refresh_trigger_second", "0 ~ 59", "1", "0",
                "60");
        testAutoRefreshConfigTimeModeFail("DAY", "1", "snapshot_automatic_refresh_trigger_second", "0 ~ 59", "1", "0",
                "-1");
    }

    private void testAutoRefreshConfigTimeModeFail(String mode, String interval, String name, String message,
            String hours, String minute, String second) {
        try {
            var snapshotConfigRequest = new SnapshotConfigRequest();
            snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
            snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
            snapshotConfigRequest.setSnapshotAutoRefreshTimeMode(mode);
            snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval(interval);
            snapshotConfigRequest.setSnapshotAutoRefreshTriggerHours(hours);
            snapshotConfigRequest.setSnapshotAutoRefreshTriggerMinute(minute);
            snapshotConfigRequest.setSnapshotAutoRefreshTriggerSecond(second);
            projectService.checkSnapshotAutoRefreshConfig(snapshotConfigRequest);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            var expected = new KylinException(PARAMETER_INVALID_SUPPORT_LIST, name, message);
            assertEquals(expected.getErrorCodeString(), ((KylinException) e).getErrorCodeString());
            assertEquals(expected.getLocalizedMessage(), e.getLocalizedMessage());
        }
    }

    @Test
    void testAutoRefreshConfigTimeMode() {
        testAutoRefreshConfigTimeMode("HOURS", "1", null, null, null);
        testAutoRefreshConfigTimeMode("HOURS", "23", null, null, null);
        testAutoRefreshConfigTimeMode("MINUTE", "1", null, null, null);
        testAutoRefreshConfigTimeMode("MINUTE", "59", null, null, null);
        testAutoRefreshConfigTimeMode("DAY", "59", "0", "0", "0");
        testAutoRefreshConfigTimeMode("DAY", "4", "1", "2", "3");
    }

    private void testAutoRefreshConfigTimeMode(String mode, String interval, String second, String minute,
            String hours) {
        var snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode(mode);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval(interval);
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerSecond(second);
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerMinute(minute);
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerHours(hours);
        projectService.checkSnapshotAutoRefreshConfig(snapshotConfigRequest);
    }

    @Test
    void testCreateCronRefreshEnabledDefault() {
        val snapshotConfigRequest = new SnapshotConfigRequest();
        var snapshotAutoRefreshCron = projectService.createSnapshotAutoRefreshCron(snapshotConfigRequest);
        assertNull(snapshotAutoRefreshCron);
    }

    @Test
    void testCreateCronTimeModeMinute() {
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        val minute = "2";
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("MINUTE");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval(minute);
        var snapshotAutoRefreshCron = projectService.createSnapshotAutoRefreshCron(snapshotConfigRequest);
        assertEquals("0 */" + minute + " * * * ?", snapshotAutoRefreshCron);
    }

    @Test
    void testCreateCronTimeModeHours() {
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        val hours = "3";
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval(hours);
        var snapshotAutoRefreshCron = projectService.createSnapshotAutoRefreshCron(snapshotConfigRequest);
        assertEquals("0 0 */" + hours + " * * ?", snapshotAutoRefreshCron);
    }

    @Test
    void testCreateCronTimeModeDay() {
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        val second = "1";
        val minute = "2";
        val hours = "3";
        val day = "4";
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("DAY");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval(day);
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerHours(hours);
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerMinute(minute);
        snapshotConfigRequest.setSnapshotAutoRefreshTriggerSecond(second);
        val snapshotAutoRefreshCron = projectService.createSnapshotAutoRefreshCron(snapshotConfigRequest);
        assertEquals(second + " " + minute + " " + hours + " */" + day + " * ?", snapshotAutoRefreshCron);
    }

    @Test
    void testCreateCronTimeModeError() {
        try {
            val snapshotConfigRequest = new SnapshotConfigRequest();
            snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
            snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("123");
            projectService.createSnapshotAutoRefreshCron(snapshotConfigRequest);
            fail();
        } catch (Exception e) {
            val expected = new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "snapshot_automatic_refresh_time_mode",
                    "DAY, HOURS, MINUTE");
            assertTrue(e instanceof KylinException);
            assertEquals(expected.getErrorCodeString(), ((KylinException) e).getErrorCodeString());
            assertEquals(expected.getLocalizedMessage(), e.getLocalizedMessage());
        }
    }

    @Test
    void testCreateCronTimeIntervalError() {
        try {
            val snapshotConfigRequest = new SnapshotConfigRequest();
            snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
            snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("MINUTE");
            snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("minute");
            projectService.createSnapshotAutoRefreshCron(snapshotConfigRequest);
            fail();
        } catch (Exception e) {
            val expected = new KylinException(CONFIG_NOT_SUPPORT_EDIT, "kylin.snapshot.automatic_refresh_cron");
            assertTrue(e instanceof KylinException);
            assertEquals(expected.getErrorCodeString(), ((KylinException) e).getErrorCodeString());
            assertEquals(expected.getLocalizedMessage(), e.getLocalizedMessage());
        }
    }

    @Test
    void testSnapshotConfigDefault() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());
        var project = projectManager.getProject(projectName);
        assertFalse(project.getConfig().isSnapshotAutoRefreshEnabled());
        assertEquals("0 0 0 */1 * ?", project.getConfig().getSnapshotAutoRefreshCron());
    }

    @Test
    void testUpdateSnapshotConfigManagementEnabledDefaultValue() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());
        var project = projectManager.getProject(projectName);

        val snapshotConfigRequest = new SnapshotConfigRequest();
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
        project = projectManager.getProject(projectName);
        assertFalse(project.getConfig().isSnapshotAutoRefreshEnabled());
        assertEquals("0 0 0 */1 * ?", project.getConfig().getSnapshotAutoRefreshCron());
    }

    @Test
    void testUpdateSnapshotConfigRefreshEnabledDefaultValue() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());
        var project = projectManager.getProject(projectName);

        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
        project = projectManager.getProject(projectName);
        assertFalse(project.getConfig().isSnapshotAutoRefreshEnabled());
        assertEquals("0 0 0 */1 * ?", project.getConfig().getSnapshotAutoRefreshCron());
    }

    @Test
    void testUpdateSnapshotConfig() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());
        var project = projectManager.getProject(projectName);

        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        // open automatic refresh
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
        project = projectManager.getProject(projectName);
        assertTrue(project.getConfig().isSnapshotAutoRefreshEnabled());
        assertEquals("0 0 */1 * * ?", project.getConfig().getSnapshotAutoRefreshCron());
    }

    @Test
    void testRefreshSnapshotDisabledToDisabled() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());

        val snapshotConfigRequest = new SnapshotConfigRequest();
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
    }

    @Test
    void testRefreshSnapshotDisabledToEnabled() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());

        // open automatic refresh
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
    }

    @Test
    void testRefreshSnapshotDisabledToEnabled2() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());

        var snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        // open automatic refresh
        snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
    }

    @Test
    void testRefreshSnapshotEnabledToEnabled() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());

        // open automatic refresh
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        // nothing to change
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        // update cron
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("MINUTE");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
    }

    @Test
    void testRefreshSnapshotEnabledToDisabled() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());

        // open automatic refresh
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        // disable automatic refresh
        snapshotConfigRequest.setSnapshotManualManagementEnabled(false);
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
    }

    @Test
    void testRefreshSnapshotEnabledToDisabled2() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());

        // open automatic refresh
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(true);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        // disable automatic refresh
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(false);
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);
    }

    @Test
    void testSetSnapshotAutoRefreshParamsRefreshEnabledDefault() {
        val response = new ProjectConfigResponse();

        projectService.setSnapshotAutoRefreshParams(response,
                KylinConfig.getInstanceFromEnv().getSnapshotAutoRefreshCron());
        assertEquals("DAY", response.getSnapshotAutoRefreshTimeMode());
        assertEquals("1", response.getSnapshotAutoRefreshTimeInterval());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerHours());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerMinute());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerSecond());
    }

    @Test
    void testSetSnapshotAutoRefreshParamsWithCronEmpty() {
        try {
            val response = new ProjectConfigResponse();
            response.setSnapshotAutoRefreshEnabled(true);
            projectService.setSnapshotAutoRefreshParams(response, "");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals(String.format("Cron expression must consist of 6 fields (found %d in \"%s\")", 0, ""),
                    e.getMessage());
        }
    }

    @Test
    void testSetSnapshotAutoRefreshParamsWithCronMinute() {
        val response = new ProjectConfigResponse();
        response.setSnapshotAutoRefreshEnabled(true);
        var cron = "0 */2 * * * ?";
        projectService.setSnapshotAutoRefreshParams(response, cron);
        assertEquals("MINUTE", response.getSnapshotAutoRefreshTimeMode());
        assertEquals("2", response.getSnapshotAutoRefreshTimeInterval());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerHours());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerMinute());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerSecond());
    }

    @Test
    void testSetSnapshotAutoRefreshParamsWithCronHours() {
        val response = new ProjectConfigResponse();
        response.setSnapshotAutoRefreshEnabled(true);
        var cron = "0 0 */3 * * ?";
        projectService.setSnapshotAutoRefreshParams(response, cron);
        assertEquals("HOURS", response.getSnapshotAutoRefreshTimeMode());
        assertEquals("3", response.getSnapshotAutoRefreshTimeInterval());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerHours());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerMinute());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerSecond());
    }

    @Test
    void testSetSnapshotAutoRefreshParamsWithCronDay() {
        val response = new ProjectConfigResponse();
        response.setSnapshotAutoRefreshEnabled(true);
        var cron = "1 2 3 */4 * ?";
        projectService.setSnapshotAutoRefreshParams(response, cron);
        assertEquals("DAY", response.getSnapshotAutoRefreshTimeMode());
        assertEquals("4", response.getSnapshotAutoRefreshTimeInterval());
        assertEquals("3", response.getSnapshotAutoRefreshTriggerHours());
        assertEquals("2", response.getSnapshotAutoRefreshTriggerMinute());
        assertEquals("1", response.getSnapshotAutoRefreshTriggerSecond());
    }

    @Test
    void testSnapshotAutoRefreshParams() {
        val projectName = RandomUtil.randomUUIDStr();
        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, Maps.newLinkedHashMap());

        var response = projectService.getProjectConfig0(projectName);
        assertFalse(response.isSnapshotAutoRefreshEnabled());
        assertEquals("DAY", response.getSnapshotAutoRefreshTimeMode());
        assertEquals("1", response.getSnapshotAutoRefreshTimeInterval());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerHours());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerMinute());
        assertEquals("0", response.getSnapshotAutoRefreshTriggerSecond());
    }

    @Test
    void testUpdateProjectConfig() {
        testUpdateProjectConfigRefreshMaxJob(false, false, "20");
        testUpdateProjectConfigRefreshMaxJob(false, true, "1");
        testUpdateProjectConfigRefreshMaxJob(true, false, "20");
        testUpdateProjectConfigRefreshMaxJob(true, true, "20");
        testUpdateProjectConfigRefreshMaxJob(true, true, "1");
    }

    private void testUpdateProjectConfigRefreshMaxJob(boolean manualManagement, boolean automaticRefresh,
            String maxConcurrentJobs) {
        val projectName = RandomUtil.randomUUIDStr();
        val testOverride = Maps.<String, String> newLinkedHashMap();

        val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject(projectName, "test", null, testOverride);
        val snapshotConfigRequest = new SnapshotConfigRequest();
        snapshotConfigRequest.setSnapshotManualManagementEnabled(manualManagement);
        snapshotConfigRequest.setSnapshotAutoRefreshEnabled(automaticRefresh);
        snapshotConfigRequest.setSnapshotAutoRefreshTimeMode("HOURS");
        snapshotConfigRequest.setSnapshotAutoRefreshTimeInterval("1");
        projectService.updateSnapshotConfig(projectName, snapshotConfigRequest);

        testOverride.put(" testk1 ", " testv1 ");
        projectService.updateProjectConfig(projectName, testOverride);
        val config = projectManager.getProject(projectName).getConfig();
        val kylinConfigExt = config.getExtendedOverrides();
        assertEquals("testv1", kylinConfigExt.get("testk1"));
        assertEquals(20, config.getSnapshotAutoRefreshMaxConcurrentJobLimit());

        testOverride.put("kylin.snapshot.auto-refresh-max-concurrent-jobs", maxConcurrentJobs);
        projectService.updateProjectConfig(projectName, testOverride);
        assertEquals(Integer.parseInt(maxConcurrentJobs),
                projectManager.getProject(projectName).getConfig().getSnapshotAutoRefreshMaxConcurrentJobLimit());
    }
}
