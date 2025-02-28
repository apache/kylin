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

package org.apache.kylin.newten;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.util.ExecAndComp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NCountDistinctWithoutEncodeTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        JobContextUtil.cleanUp();
        setOverlay("src/test/resources/ut_meta/count_distinct_no_encode");

        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/count_distinct_no_encode" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "count_distinct_no_encode";
    }

    @Test
    public void testWithoutEncode() throws Exception {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        indexPlanManager.updateIndexPlan("b06eee9f-3e6d-41de-ac96-89dbf170b99b",
                copyForWrite -> copyForWrite.getOverrideProps().put("kylin.query.skip-encode-integer-enabled", "true"));
        fullBuild("b06eee9f-3e6d-41de-ac96-89dbf170b99b");
        List<String> results1 = ExecAndComp
                .queryModel(getProject(),
                        "select city, " + "count(distinct string_id), " + "count(distinct tinyint_id), "
                                + "count(distinct smallint_id), " + "count(distinct int_id), "
                                + "count(distinct bigint_id) from test_count_distinct group by city order by city")
                .collectAsList().stream().map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals(3, results1.size());
        Assert.assertEquals("上海,4,4,4,4,4", results1.get(0));
        Assert.assertEquals("北京,3,3,3,3,3", results1.get(1));
        Assert.assertEquals("广州,5,5,5,5,5", results1.get(2));

        List<String> results2 = ExecAndComp
                .queryModel(getProject(),
                        "select " + "count(distinct string_id), " + "count(distinct tinyint_id), "
                                + "count(distinct smallint_id), " + "count(distinct int_id), "
                                + "count(distinct bigint_id) from test_count_distinct")
                .collectAsList().stream().map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals(1, results2.size());
        Assert.assertEquals("5,5,5,5,5", results2.get(0));

        String dictPath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/" + getProject()
                + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/DEFAULT.TEST_COUNT_DISTINCT";
        FileStatus[] fileStatuses = new Path(dictPath).getFileSystem(new Configuration())
                .listStatus(new Path(dictPath));
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertEquals("STRING_ID", fileStatuses[0].getPath().getName());
    }

    @Test
    public void testWithEncode() throws Exception {
        fullBuild("b06eee9f-3e6d-41de-ac96-89dbf170b99b");
        String dictPath = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/" + getProject()
                + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/DEFAULT.TEST_COUNT_DISTINCT";
        FileStatus[] fileStatuses = new Path(dictPath).getFileSystem(new Configuration())
                .listStatus(new Path(dictPath));
        Assert.assertEquals(5, fileStatuses.length);

        String[] expected = { "BIGINT_ID", "INT_ID", "SMALLINT_ID", "STRING_ID", "TINYINT_ID" };
        Assert.assertArrayEquals(expected,
                Arrays.stream(fileStatuses).map(fileStatus -> fileStatus.getPath().getName()).sorted().toArray());
    }

}
