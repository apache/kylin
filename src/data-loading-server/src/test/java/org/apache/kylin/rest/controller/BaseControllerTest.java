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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_CONFLICT_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_LESS_THAN_ZERO;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_CONSISTENT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_FORMAT_MS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.controller.fake.HandleErrorController;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.util.DataRangeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@RunWith(MockitoJUnitRunner.class)
public class BaseControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private final BaseController baseController = Mockito.spy(new BaseController());

    private final HandleErrorController handleErrorController = Mockito.spy(new HandleErrorController());

    private ProjectService projectService;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(handleErrorController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        Mockito.when(handleErrorController.request()).thenThrow(new RuntimeException(), new ForbiddenException(),
                new NotFoundException(StringUtils.EMPTY), new AccessDeniedException(StringUtils.EMPTY),
                new UnauthorizedException(USER_AUTH_INFO_NOTFOUND),
                new KylinException(UNKNOWN_ERROR_CODE, StringUtils.EMPTY));
        createTestMetadata();

        projectService = Mockito.mock(ProjectService.class);
        ReflectionTestUtils.setField(baseController, "projectService", projectService);
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testHandleErrors() throws Exception {
        // assert handleError
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        // assert handleForbidden
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isForbidden());

        // assert handleNotFound
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isNotFound());

        // assert handleAccessDenied
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        // assert handleUnauthorized
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isUnauthorized());

        // assert handleErrorCode
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
    }

    @Test
    public void testCheckProjectException() {
        thrown.expect(KylinException.class);
        baseController.checkProjectName("");
    }

    @Test
    public void testCheckProjectPass() {
        baseController.checkProjectName("default");
        assert true;
    }

    @Test
    public void testCheckRequiredArgPass() {
        baseController.checkRequiredArg("model", "modelId");
        assert true;
    }

    @Test
    public void testCheckRequiredArgException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("model"));
        baseController.checkRequiredArg("model", "");
    }

    @Test
    public void testCheckStartAndEndException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_NOT_CONSISTENT.getMsg());
        DataRangeUtils.validateDataRange("10", "");
    }

    @Test
    public void testTimeRangeEndGreaterThanStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        DataRangeUtils.validateDataRange("10", "1");
    }

    @Test
    public void testTimeRangeEndEqualToStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        DataRangeUtils.validateDataRange("1", "1");
    }

    @Test
    public void testTimeRangeInvalidStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_LESS_THAN_ZERO.getMsg());
        DataRangeUtils.validateDataRange("-1", "1");
    }

    @Test
    public void testTimeRangeInvalidEnd() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_LESS_THAN_ZERO.getMsg());
        DataRangeUtils.validateDataRange("2", "-1");
    }

    @Test
    public void testTimeRangeInvalidFormat() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_NOT_FORMAT_MS.getMsg());
        DataRangeUtils.validateDataRange("start", "end");
    }

    @Test
    public void testTimeRangeValid() {
        DataRangeUtils.validateDataRange("0", "86400000", "yyyy-MM-dd");
        DataRangeUtils.validateDataRange("1000000000000", "2200000000000", "yyyy-MM-dd");
        DataRangeUtils.validateDataRange("0", "86400000", PartitionDesc.TimestampType.MILLISECOND.name);
        DataRangeUtils.validateDataRange("1000000000000", "2200000000000", PartitionDesc.TimestampType.SECOND.name);
    }

    @Test
    public void testTimeRangeEndEqualToStartWithDateFormat() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        String start = null;
        String end = null;
        try {
            start = Long.toString(format.parse("2012-01-01 00:00:00").getTime());
            end = Long.toString(format.parse("2012-01-01 06:00:00").getTime());
        } catch (Exception e) {
        }
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        DataRangeUtils.validateDataRange(start, end, "yyyy-MM-dd");
    }

    @Test
    public void testFormatStatus() {
        List<String> status = Lists.newArrayList("OFFLINE", null, "broken");
        assertEquals(baseController.formatStatus(status, ModelStatusToDisplayEnum.class),
                Lists.newArrayList("OFFLINE", "BROKEN"));

        thrown.expect(KylinException.class);
        thrown.expectMessage(PARAMETER_INVALID_SUPPORT_LIST.getMsg("status", "ONLINE, OFFLINE, WARNING, BROKEN"));
        status = Lists.newArrayList("OFF", null, "broken");
        baseController.formatStatus(status, ModelStatusToDisplayEnum.class);
    }

    @Test
    public void testCheckParamLength() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Message.getInstance().getParamTooLarge(), "tag", 1000));
        List<Object> param = new ArrayList();
        param.add(1);
        param.add(6);
        param.add(String.join("", Collections.nCopies(1000 * 1024, "l")));
        baseController.checkParamLength("tag", param, 1000);
    }

    @Test
    public void testGetProject() {
        Mockito.when(projectService.getReadableProjects(Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(Collections.emptyList());
        try {
            baseController.getProject("SOME_PROJECT");
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(PROJECT_NOT_EXIST.getCodeMsg("SOME_PROJECT"), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckProjectName() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = Mockito.mockStatic(KylinConfig.class);
                MockedStatic<NProjectManager> nProjectManagerMockedStatic = Mockito.mockStatic(NProjectManager.class)) {
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(Mockito.mock(KylinConfig.class));
            NProjectManager projectManager = Mockito.mock(NProjectManager.class);
            nProjectManagerMockedStatic.when(() -> NProjectManager.getInstance(Mockito.any()))
                    .thenReturn(projectManager);
            Mockito.when(projectManager.getProject(Mockito.anyString())).thenReturn(null);

            try {
                baseController.checkProjectName("SOME_PROJECT");
                Assert.fail();
            } catch (Exception e) {
                assertTrue(e instanceof KylinException);
                assertEquals(PROJECT_NOT_EXIST.getCodeMsg("SOME_PROJECT"), e.getLocalizedMessage());
            }
        }
    }

    @Test
    public void testCheckSegmentParams() {
        try {
            baseController.checkSegmentParams(new String[] { "id1" }, new String[] { "name1" });
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(SEGMENT_CONFLICT_PARAMETER.getCodeMsg(), e.getLocalizedMessage());
        }

        try {
            baseController.checkSegmentParams(null, null);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(SEGMENT_EMPTY_PARAMETER.getCodeMsg(), e.getLocalizedMessage());
        }
    }

    @Test
    public void testNeedRoutedToOtherInstance() {
        Map<String, List<String>> nodeWithJobs = new HashMap<>();

        // all jobs running on current node
        nodeWithJobs.put(AddressUtil.getLocalInstance(), Lists.newArrayList("job1", "job2"));
        assertFalse(baseController.needRouteToOtherInstance(nodeWithJobs, JobActionEnum.RESUME.name()));
        assertFalse(baseController.needRouteToOtherInstance(nodeWithJobs, JobActionEnum.PAUSE.name()));

        // some jobs running on other nodes
        nodeWithJobs.put("node1", Lists.newArrayList("job1", "job2"));
        nodeWithJobs.put("node2", Lists.newArrayList("job3", "job4"));

        assertFalse(baseController.needRouteToOtherInstance(nodeWithJobs, JobActionEnum.RESUME.name()));
        assertTrue(baseController.needRouteToOtherInstance(nodeWithJobs, JobActionEnum.PAUSE.name()));

        // all jobs running on other nodes
        nodeWithJobs.clear();
        nodeWithJobs.put("node1", Lists.newArrayList("job1", "job2"));
        assertFalse(baseController.needRouteToOtherInstance(nodeWithJobs, JobActionEnum.RESUME.name()));
        assertTrue(baseController.needRouteToOtherInstance(nodeWithJobs, JobActionEnum.PAUSE.name()));
    }

    @Test
    public void testTimeOverlapScenarios() {
        // Test 1: Full build condition
        List<String[]> rangeList1 = Collections.singletonList(new String[] { "0", "0" });
        String[] range1 = { "2023-01-01", "2023-01-02" };
        assertTrue("Full build condition failed", DataRangeUtils.timeOverlap(rangeList1, range1, "yyyy-MM-dd"));

        // Test 2: Range is null
        List<String[]> rangeList2 = Lists.newArrayList();
        rangeList2.add(new String[] { "2023-01-01", "2023-01-02" });
        // Test 3: Range length not two
        String[] range3 = { "2023-01-01" };
        assertFalse("Range length not two condition failed",
                DataRangeUtils.timeOverlap(rangeList2, range3, "yyyy-MM-dd"));

        // Test 4: Empty date format
        String[] range4 = { "2023-01-01", "2023-01-02" };
        assertFalse("Empty date format condition failed", DataRangeUtils.timeOverlap(rangeList2, range4, ""));

        // Test 5: New range zero condition
        String[] range5 = { "0", "0" };
        assertTrue("New range zero condition failed", DataRangeUtils.timeOverlap(rangeList2, range5, "yyyy-MM-dd"));

        // Test 6: Valid overlap
        List<String[]> rangeList6 = Lists.newArrayList();
        rangeList6.add(new String[] { "2023-01-01", "2023-01-03" });
        String[] range6 = { "2023-01-02", "2023-01-04" };
        assertTrue("Valid overlap condition failed", DataRangeUtils.timeOverlap(rangeList6, range6, "yyyy-MM-dd"));

        // Test 7: No overlap
        String[] range7 = { "2023-01-03", "2023-01-04" };
        assertFalse("No overlap condition failed", DataRangeUtils.timeOverlap(rangeList6, range7, "yyyy-MM-dd"));

        // Test 8: Invalid time range
        try {
            String[] range8 = { "2023-01-02", "2023-01-01" };
            DataRangeUtils.timeOverlap(rangeList6, range8, "yyyy-MM-dd");
            fail("Invalid time range did not throw exception");
        } catch (IllegalArgumentException e) {
            // Expected behavior
        }

        // Test 9: Invalid date format
        List<String[]> rangeList9 = Lists.newArrayList();
        rangeList9.add(new String[] { "2023-01-01", "2023-01-02" });
        String[] range9 = { "01-2023-01", "2023-01-02" };
        assertFalse("Invalid date format condition failed",
                DataRangeUtils.timeOverlap(rangeList9, range9, "yyyy-MM-dd"));
    }

    @Test
    public void testMergeTimeRange() throws ParseException {
        // Test Year Format
        List<String> valuesYear = Arrays.asList("2001", "2002", "2003", "2005", "2007", "2008");
        List<String[]> expectedYear = Arrays.asList(new String[] { "2001", "2004" }, new String[] { "2005", "2006" },
                new String[] { "2007", "2009" });
        List<String[]> resultYear = DataRangeUtils.mergeTimeRange(valuesYear, "yyyy");
        assertTrue("Year format merge failed", deepEquals(expectedYear, resultYear));

        // Test Day Format
        List<String> valuesDay = Arrays.asList("2023-01-01", "2023-01-02", "2023-01-03", "2023-01-05", "2023-01-07",
                "2023-01-08");
        List<String[]> expectedDay = Arrays.asList(new String[] { "2023-01-01", "2023-01-04" },
                new String[] { "2023-01-05", "2023-01-06" }, new String[] { "2023-01-07", "2023-01-09" });
        List<String[]> resultDay = DataRangeUtils.mergeTimeRange(valuesDay, "yyyy-MM-dd");
        assertTrue("Day format merge failed", deepEquals(expectedDay, resultDay));

        // Test Month Format
        List<String> valuesMonth = Arrays.asList("2023-01", "2023-02", "2023-03", "2023-05", "2024-01", "2024-02");
        List<String[]> expectedMonth = Arrays.asList(new String[] { "2023-01", "2023-04" },
                new String[] { "2023-05", "2023-06" }, new String[] { "2024-01", "2024-03" });
        List<String[]> resultMonth = DataRangeUtils.mergeTimeRange(valuesMonth, "yyyy-MM");
        assertTrue("Month format merge failed", deepEquals(expectedMonth, resultMonth));

        // Test Empty List
        List<String> valuesEmpty = Collections.emptyList();
        List<String[]> expectedEmpty = Collections.emptyList();
        List<String[]> resultEmpty = DataRangeUtils.mergeTimeRange(valuesEmpty, "yyyy");
        assertTrue("Empty list merge failed", deepEquals(expectedEmpty, resultEmpty));

        List<String> valuesUnsupported = Arrays.asList("2023-01-01");
        assertThrows(IllegalArgumentException.class,
                () -> DataRangeUtils.mergeTimeRange(valuesUnsupported, "unsupported-format"));
    }

    private boolean deepEquals(List<String[]> list1, List<String[]> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }
        for (int i = 0; i < list1.size(); i++) {
            if (!Arrays.equals(list1.get(i), list2.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testTimeInRange() {
        // Test 1: Data range within partition range
        String[] dataRange1 = { "2023-01-05", "2023-01-10" };
        List<String[]> partitionRange1 = Arrays.asList(new String[] { "2023-01-01", "2023-01-15" },
                new String[] { "2022-12-01", "2023-02-01" });
        assertTrue("Data range should be within partition range",
                DataRangeUtils.timeInRange(dataRange1, partitionRange1, "yyyy-MM-dd"));

        // Test 2: Data range outside partition range
        String[] dataRange2 = { "2023-01-05", "2023-01-10" };
        List<String[]> partitionRange2 = Arrays.asList(new String[] { "2023-01-11", "2023-01-15" },
                new String[] { "2023-01-16", "2023-01-20" });
        assertFalse("Data range should be outside partition range",
                DataRangeUtils.timeInRange(dataRange2, partitionRange2, "yyyy-MM-dd"));

        // Test 3: Invalid data format
        String[] dataRange3 = { "2023-0105", "2023-0110" };
        assertFalse("Data range with invalid format should return false",
                DataRangeUtils.timeInRange(dataRange3, partitionRange1, "yyyy-MM-dd"));

        // Test 4: Invalid data range (start after end)
        String[] dataRange4 = { "2023-01-10", "2023-01-05" };
        assertFalse("Invalid data range (start after end) should return false",
                DataRangeUtils.timeInRange(dataRange4, partitionRange1, "yyyy-MM-dd"));

        // Test 5: Null or empty inputs
        assertFalse("Null data range should return false",
                DataRangeUtils.timeInRange(null, partitionRange1, "yyyy-MM-dd"));
        assertFalse("Empty data range should return false",
                DataRangeUtils.timeInRange(new String[] {}, partitionRange1, "yyyy-MM-dd"));
        assertFalse("Null partition range should return false",
                DataRangeUtils.timeInRange(dataRange1, null, "yyyy-MM-dd"));
        assertFalse("Empty partition range should return false",
                DataRangeUtils.timeInRange(dataRange1, Collections.emptyList(), "yyyy-MM-dd"));
        assertFalse("Empty format string should return false",
                DataRangeUtils.timeInRange(dataRange1, partitionRange1, ""));
    }
}
