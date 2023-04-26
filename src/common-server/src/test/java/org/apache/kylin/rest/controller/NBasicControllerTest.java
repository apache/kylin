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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.DATETIME_FORMAT_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.DATETIME_FORMAT_PARSE_ERROR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.INTEGER_NON_NEGATIVE_CHECK;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_LESS_THAN_ZERO;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_CONSISTENT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_FORMAT_MS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.controller.fixture.FixtureController;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.util.DataRangeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NBasicControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NBasicController nBasicController = Mockito.spy(new NBasicController());

    private final FixtureController fixtureController = Mockito.spy(new FixtureController());

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(fixtureController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        Mockito.when(fixtureController.request()).thenThrow(new RuntimeException(), new ForbiddenException(),
                new NotFoundException(StringUtils.EMPTY), new AccessDeniedException(StringUtils.EMPTY),
                new UnauthorizedException(USER_AUTH_INFO_NOTFOUND),
                new KylinException(UNKNOWN_ERROR_CODE, StringUtils.EMPTY));
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testValidatePriority() {
        Assert.assertThrows(KylinException.class, () -> NBasicController.validatePriority(9999));

    }

    @Test
    public void testCheckId() {
        Assert.assertThrows(KylinException.class, () -> NBasicController.checkId(""));

    }

    @Test
    public void testCheckSqlIsNotNull() {
        Assert.assertThrows(KylinException.class, () -> NBasicController.checkSqlIsNotNull(null));

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
    public void testGetProject_throwsException() {
        try {
            nBasicController.getProject(null);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(PROJECT_NOT_EXIST.getCodeMsg(null), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckProjectException() {
        thrown.expect(KylinException.class);
        nBasicController.checkProjectName("");
    }

    @Test
    public void testCheckProjectPass() {
        nBasicController.checkProjectName("default");
        assert true;
    }

    @Test
    public void testCheckRequiredArgPass() {
        nBasicController.checkRequiredArg("model", "modelId");
        assert true;
    }

    @Test
    public void testCheckRequiredArgException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("model"));
        nBasicController.checkRequiredArg("model", "");
    }

    @Test
    public void testCheckStartAndEndException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_NOT_CONSISTENT.getMsg());
        nBasicController.validateDataRange("10", "");
    }

    @Test
    public void testTimeRangeEndGreaterThanStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        nBasicController.validateDataRange("10", "1");
    }

    @Test
    public void testTimeRangeEndEqualToStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        nBasicController.validateDataRange("1", "1");
    }

    @Test
    public void testTimeRangeInvalidStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_LESS_THAN_ZERO.getMsg());
        nBasicController.validateDataRange("-1", "1");
    }

    @Test
    public void testTimeRangeInvalidEnd() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_LESS_THAN_ZERO.getMsg());
        nBasicController.validateDataRange("2", "-1");
    }

    @Test
    public void testTimeRangeInvalidFormat() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(TIME_INVALID_RANGE_NOT_FORMAT_MS.getMsg());
        nBasicController.validateDataRange("start", "end");
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
    public void testCheckParamLength() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Message.getInstance().getParamTooLarge(), "tag", 1000));
        List param = new ArrayList();
        param.add(1);
        param.add(6);
        param.add(String.join("", Collections.nCopies(1000, "l")));
        nBasicController.checkParamLength("tag", param, 1000);
    }

    @Test
    public void testCheckStreamingEnabled() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getStreamingDisabled());
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        nBasicController.checkStreamingEnabled();
    }

    @Test
    public void testCheckSegmentParms_throwsException() {
        String[] ids = new String[] { "TEST_ID1" };
        String[] names = new String[] { "TEST_NAME1" };

        // test throwing SEGMENT_CONFLICT_PARAMETER
        try {
            nBasicController.checkSegmentParms(ids, names);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-010022214: Can't enter segment ID and name at the same time. Please re-enter.",
                    e.toString());
        }

        // test throwing SEGMENT_EMPTY_PARAMETER
        try {
            nBasicController.checkSegmentParms(null, null);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-010022215: Please enter segment ID or name.", e.toString());
        }
    }

    @Test
    public void testValidateDateTimeFormatPattern() {
        Assert.assertThrows(DATETIME_FORMAT_EMPTY.getMsg(), KylinException.class,
                () -> nBasicController.validateDateTimeFormatPattern(""));
        Assert.assertThrows(DATETIME_FORMAT_PARSE_ERROR.getMsg("AABBSS"), KylinException.class,
                () -> nBasicController.validateDateTimeFormatPattern("AABBSS"));
    }

    @Test
    public void testCheckNonNegativeIntegerArg() {
        Assert.assertThrows(INTEGER_NON_NEGATIVE_CHECK.getMsg(), KylinException.class,
                () -> nBasicController.checkNonNegativeIntegerArg("id", -1));
    }

    @Test
    public void testSetCustomDataResponse() {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName("table1");
        Map<String, Object> mockDataResponse = nBasicController.setCustomDataResponse("table",
                Pair.newPair(Collections.singletonList(tableDesc), 3), 0, 10);
        Assert.assertNotNull(mockDataResponse);
        Object tableData = mockDataResponse.get("table");
        if (tableData instanceof List<?>) {
            for (Object tableDatum : (List<?>) tableData) {
                Assert.assertEquals("table1", ((TableDesc)tableDatum).getName().toLowerCase(Locale.ROOT));
            }
        }
        Assert.assertEquals(3, mockDataResponse.get("size"));
    }
    
    @Test
    public void testEncodeAndDecodeHost() {
        Assert.assertTrue(nBasicController.encodeHost("").isEmpty());
        String host = "localhost:7070";
        String encodeHost = nBasicController.encodeHost(host);
        Assert.assertNotNull(encodeHost);
        Assert.assertNotEquals(host, encodeHost);
        String decodeHost = nBasicController.decodeHost(encodeHost);
        Assert.assertEquals("http://" + host, decodeHost);
        Assert.assertEquals("ip", nBasicController.decodeHost("ip"));
    }

}
