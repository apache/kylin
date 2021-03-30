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

package org.apache.kylin.job;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Zhixin Liu (liuzx32)
 */
public class JoinedFormatterTest extends LocalFileMetadataTestCase {

    private JoinedFormatter formatter;

    @Before
    public void setUp() throws Exception {
        formatter = new JoinedFormatter();
        formatter.setStartDate("20190710");
        formatter.setEndDate("20190711");
    }

    @After
    public void after() throws Exception {
        formatter.printEnv();
    }

    @Test
    public void testConditionFormat() {
        String expected = "date_str>='20190710' and date_str<'20190711'";
        String condition = "date_str>='${start_date}' and date_str<'${end_date}'";
        String fmtCondition = formatter.formatSentence(condition);
        assertEquals(expected, fmtCondition);
    }

    @Test
    public void testSqlFormat() {
        String expected = "select * from table where date_str>=20190710 and date_str<20190711";
        String sql = "select * from table where date_str>=${start_date} and date_str<${end_date}";
        String fmtSql = formatter.formatSentence(sql);
        assertEquals(expected, fmtSql);
    }

    @Test
    // For JIRA: KYLIN-4229
    public void testDateFormatWithFullBuildAndFilter() {
        this.createTestMetadata();
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("fifty_dim_full_build_cube");
        CubeSegment mockSeg = new CubeSegment();

        mockSeg.setCubeInstance(cube);
        mockSeg.setName("FULL_BUILD");
        mockSeg.setUuid(RandomUtil.randomUUID().toString());
        mockSeg.setStorageLocationIdentifier(RandomUtil.randomUUID().toString());
        mockSeg.setStatus(SegmentStatusEnum.READY);
        mockSeg.setTSRange(new SegmentRange.TSRange(0L, Long.MAX_VALUE));
        IJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(mockSeg);
        DataModelDesc model = flatTableDesc.getDataModel();
        model.setFilterCondition("A > 1");

        JoinedFlatTable.generateSelectDataStatement(flatTableDesc);
    }
}
