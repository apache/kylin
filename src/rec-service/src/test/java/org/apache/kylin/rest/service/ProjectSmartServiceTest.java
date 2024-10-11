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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.ProjectStatisticsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

public class ProjectSmartServiceTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @InjectMocks
    private final ProjectSmartService projectSmartService = Mockito.spy(ProjectSmartService.class);

    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @InjectMocks
    private final RawRecService rawRecService = Mockito.spy(RawRecService.class);

    @InjectMocks
    private final UserService userService = Mockito.spy(UserService.class);

    @InjectMocks
    private final NUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @InjectMocks
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Spy
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.cube.low-frequency-threshold", "5");
        createTestMetadata();
        MockitoAnnotations.openMocks(this);
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        createRules();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
    }

    @After
    public void tearDown() {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("SHUTDOWN;");
        }
        cleanupTestMetadata();
    }

    private void createRules() {
        FavoriteRule.SQLCondition cond1 = new FavoriteRule.SQLCondition("1", "SELECT *\nFROM \"TEST_KYLIN_FACT\"");
        FavoriteRule.Condition cond2 = new FavoriteRule.Condition(null, "10");
        FavoriteRule.Condition cond3 = new FavoriteRule.Condition(null, "ROLE_ADMIN");
        FavoriteRule.Condition cond4 = new FavoriteRule.Condition(null, "userA");
        FavoriteRule.Condition cond5 = new FavoriteRule.Condition(null, "userB");
        FavoriteRule.Condition cond6 = new FavoriteRule.Condition(null, "userC");
        FavoriteRule.Condition cond7 = new FavoriteRule.Condition("5", "8");
        FavoriteRuleManager manager = FavoriteRuleManager.getInstance(PROJECT);
        manager.createRule(new FavoriteRule(Collections.singletonList(cond1), "blacklist", false));
        manager.createRule(new FavoriteRule(Collections.singletonList(cond2), "count", true));
        manager.createRule(new FavoriteRule(Collections.singletonList(cond3), "submitter_group", true));
        manager.createRule(new FavoriteRule(Arrays.asList(cond4, cond5, cond6), "submitter", true));
        manager.createRule(new FavoriteRule(Collections.singletonList(cond7), "duration", true));
    }

    @Test
    public void testGetFavoriteRules() {
        Mockito.when(userService.userExists("userA")).thenReturn(true);
        Mockito.when(userService.userExists("userB")).thenReturn(true);
        Mockito.when(userService.userExists("userC")).thenReturn(true);
        Map<String, Object> favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(true, favoriteRuleResponse.get("count_enable"));
        assertEquals(10.0f, favoriteRuleResponse.get("count_value"));
        assertEquals(Lists.newArrayList("userA", "userB", "userC"), favoriteRuleResponse.get("users"));
        assertEquals(Lists.newArrayList("ROLE_ADMIN"), favoriteRuleResponse.get("user_groups"));
        assertEquals(5L, favoriteRuleResponse.get("min_duration"));
        assertEquals(8L, favoriteRuleResponse.get("max_duration"));
        assertEquals(true, favoriteRuleResponse.get("duration_enable"));
    }

    @Test
    public void testUpdateFavoriteRules() {
        TopRecsUpdateScheduler topRecsUpdateScheduler = new TopRecsUpdateScheduler();
        ReflectionTestUtils.setField(projectSmartService, "topRecsUpdateScheduler", topRecsUpdateScheduler);
        // update with FavoriteRuleUpdateRequest and assert
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setDurationEnable(false);
        request.setMinDuration("0");
        request.setMaxDuration("10");
        request.setSubmitterEnable(false);
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));
        request.setRecommendationEnable(true);
        request.setRecommendationsValue("30");
        request.setMinHitCount("11");
        request.setEffectiveDays("11");
        request.setUpdateFrequency("3");

        Mockito.when(userService.userExists("userA")).thenReturn(true);
        Mockito.when(userService.userExists("userB")).thenReturn(true);
        Mockito.when(userService.userExists("userC")).thenReturn(true);
        projectSmartService.updateRegularRule(PROJECT, request);
        Map<String, Object> favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(false, favoriteRuleResponse.get("duration_enable"));
        assertEquals(false, favoriteRuleResponse.get("submitter_enable"));
        assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        assertEquals(Lists.newArrayList(), favoriteRuleResponse.get("user_groups"));
        assertEquals(0L, favoriteRuleResponse.get("min_duration"));
        assertEquals(10L, favoriteRuleResponse.get("max_duration"));
        assertEquals(true, favoriteRuleResponse.get("recommendation_enable"));
        assertEquals(30L, favoriteRuleResponse.get("recommendations_value"));
        assertEquals(11, favoriteRuleResponse.get("min_hit_count"));
        assertEquals(11, favoriteRuleResponse.get("effective_days"));
        assertEquals(3, favoriteRuleResponse.get("update_frequency"));

        // check user_groups
        request.setUserGroups(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"));
        projectSmartService.updateRegularRule(PROJECT, request);
        Mockito.when(userGroupService.exists("USER_GROUP1")).thenReturn(true);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        assertEquals(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"), favoriteRuleResponse.get("user_groups"));

        // assert if favorite rules' values are empty
        request.setFreqEnable(false);
        request.setFreqValue(null);
        request.setDurationEnable(false);
        request.setMinDuration(null);
        request.setMaxDuration(null);
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        Assert.assertNull(favoriteRuleResponse.get("freq_value"));
        Assert.assertNull(favoriteRuleResponse.get("min_duration"));
        Assert.assertNull(favoriteRuleResponse.get("max_duration"));
        topRecsUpdateScheduler.close();
    }

    @Test
    public void testResetFavoriteRules() {
        // reset
        projectService.resetProjectConfig(PROJECT, "favorite_rule_config");
        Map<String, Object> favoriteRules = projectSmartService.getFavoriteRules(PROJECT);

        assertEquals(false, favoriteRules.get("freq_enable"));
        assertEquals(0.1f, favoriteRules.get("freq_value"));

        assertEquals(true, favoriteRules.get("count_enable"));
        assertEquals(10.0f, favoriteRules.get("count_value"));

        assertEquals(true, favoriteRules.get("submitter_enable"));
        assertEquals(Lists.newArrayList("ADMIN"), favoriteRules.get("users"));
        assertEquals(Lists.newArrayList("ROLE_ADMIN"), favoriteRules.get("user_groups"));

        assertEquals(true, favoriteRules.get("duration_enable"));
        assertEquals(5L, favoriteRules.get("min_duration"));
        assertEquals(3600L, favoriteRules.get("max_duration"));

        assertEquals(true, favoriteRules.get("recommendation_enable"));
        assertEquals(20L, favoriteRules.get("recommendations_value"));

        assertEquals(30, favoriteRules.get("min_hit_count"));
        assertEquals(2, favoriteRules.get("effective_days"));
        assertEquals(2, favoriteRules.get("update_frequency"));

    }

    @Test
    public void testGetProjectStatistics() {
        TopRecsUpdateScheduler topRecsUpdateScheduler = new TopRecsUpdateScheduler();
        ReflectionTestUtils.setField(projectSmartService, "topRecsUpdateScheduler", topRecsUpdateScheduler);
        ProjectStatisticsResponse statisticsOfProjectDefault = projectSmartService.getProjectStatistics(PROJECT);
        assertEquals(3, statisticsOfProjectDefault.getDatabaseSize());
        assertEquals(21, statisticsOfProjectDefault.getTableSize());
        assertEquals(0, statisticsOfProjectDefault.getLastWeekQueryCount());
        assertEquals(0, statisticsOfProjectDefault.getUnhandledQueryCount());
        assertEquals(-1, statisticsOfProjectDefault.getAdditionalRecPatternCount());
        assertEquals(-1, statisticsOfProjectDefault.getRemovalRecPatternCount());
        assertEquals(-1, statisticsOfProjectDefault.getRecPatternCount());
        assertEquals(-1, statisticsOfProjectDefault.getEffectiveRuleSize());
        assertEquals(-1, statisticsOfProjectDefault.getApprovedRecCount());
        assertEquals(-1, statisticsOfProjectDefault.getApprovedAdditionalRecCount());
        assertEquals(-1, statisticsOfProjectDefault.getApprovedRemovalRecCount());
        assertEquals(8, statisticsOfProjectDefault.getModelSize());
        assertEquals(-1, statisticsOfProjectDefault.getAcceptableRecSize());
        Assert.assertFalse(statisticsOfProjectDefault.isRefreshed());
        assertEquals(-1, statisticsOfProjectDefault.getMaxRecShowSize());

        ProjectStatisticsResponse statsOfPrjStreamingTest = projectSmartService.getProjectStatistics("streaming_test");
        assertEquals(2, statsOfPrjStreamingTest.getDatabaseSize());
        assertEquals(11, statsOfPrjStreamingTest.getTableSize());
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        statsOfPrjStreamingTest = projectSmartService.getProjectStatistics("streaming_test");
        assertEquals(1, statsOfPrjStreamingTest.getDatabaseSize());
        assertEquals(6, statsOfPrjStreamingTest.getTableSize());

        topRecsUpdateScheduler.close();
    }

    @Test
    public void testGetProjectStatisticsOfGC() {
        TopRecsUpdateScheduler topRecsUpdateScheduler = new TopRecsUpdateScheduler();
        ReflectionTestUtils.setField(projectSmartService, "topRecsUpdateScheduler", topRecsUpdateScheduler);
        projectService.garbageCleanup("gc_test", 0);
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics("gc_test");
        assertEquals(1, projectStatistics.getDatabaseSize());
        assertEquals(1, projectStatistics.getTableSize());
        assertEquals(0, projectStatistics.getLastWeekQueryCount());
        assertEquals(0, projectStatistics.getUnhandledQueryCount());
        assertEquals(0, projectStatistics.getAdditionalRecPatternCount());
        assertEquals(2, projectStatistics.getRemovalRecPatternCount());
        assertEquals(2, projectStatistics.getRecPatternCount());
        assertEquals(8, projectStatistics.getEffectiveRuleSize());
        assertEquals(0, projectStatistics.getApprovedRecCount());
        assertEquals(0, projectStatistics.getApprovedAdditionalRecCount());
        assertEquals(0, projectStatistics.getApprovedRemovalRecCount());
        assertEquals(2, projectStatistics.getModelSize());
        assertEquals(2, projectStatistics.getAcceptableRecSize());
        Assert.assertFalse(projectStatistics.isRefreshed());
        assertEquals(20, projectStatistics.getMaxRecShowSize());

        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject("gc_test");
        request.setExcludeTablesEnable(true);
        request.setDurationEnable(false);
        request.setMinDuration("0");
        request.setMaxDuration("10");
        request.setSubmitterEnable(true);
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));
        request.setRecommendationEnable(true);
        request.setRecommendationsValue("30");
        request.setUpdateFrequency("1");
        projectSmartService.updateRegularRule("gc_test", request);
        ProjectStatisticsResponse projectStatistics2 = projectSmartService.getProjectStatistics("gc_test");
        assertEquals(6, projectStatistics2.getEffectiveRuleSize());
        topRecsUpdateScheduler.close();
    }

    @Test
    public void testGetStreamingProjectStatistics() {
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics("streaming_test");
        assertEquals(2, projectStatistics.getDatabaseSize());
        assertEquals(11, projectStatistics.getTableSize());
        assertEquals(0, projectStatistics.getLastWeekQueryCount());
        assertEquals(0, projectStatistics.getUnhandledQueryCount());
        assertEquals(11, projectStatistics.getModelSize());
    }
}
