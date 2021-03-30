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

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.tool.extractor.CubeMetaExtractor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class CubeMetaExtractorTest extends LocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testExtractorByPrj() throws IOException {
        folder.create();
        File tempDir = folder.getRoot();
        String tempDirAbsPath = tempDir.getAbsolutePath();
        List<String> args = new ArrayList<>();
        args.add("-destDir");
        args.add(tempDirAbsPath);
        args.add("-project");
        args.add("default");
        args.add("-compress");
        args.add("false");
        args.add("-packagetype");
        args.add("cubemeta");
        String[] cubeMetaArgs = new String[args.size()];
        args.toArray(cubeMetaArgs);

        CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
        cubeMetaExtractor.execute(cubeMetaArgs);

        File[] files = tempDir.listFiles();
        Preconditions.checkState(files.length == 1);
        String dumpDir = files[0].getAbsolutePath();
        KylinConfig instanceFromUri = KylinConfig.createInstanceFromUri(dumpDir);
        NavigableSet<String> tables = new ResourceTool().list(instanceFromUri, "table");
        NavigableSet<String> tableExds = new ResourceTool().list(instanceFromUri, "table_exd");
        Set<String> expectTbl = Sets.newHashSet(
                "/table/DEFAULT.FIFTY_DIM.json", //
                "/table/DEFAULT.STREAMING_TABLE.json", //
                "/table/DEFAULT.STREAMING_CATEGORY.json", //
                "/table/DEFAULT.TEST_ACCOUNT.json", //
                "/table/DEFAULT.TEST_CATEGORY_GROUPINGS.json", //
                "/table/DEFAULT.TEST_COUNTRY.json", //
                "/table/DEFAULT.TEST_KYLIN_FACT.json", //
                "/table/DEFAULT.TEST_ORDER.json", //
                "/table/EDW.TEST_CAL_DT.json", //
                "/table/EDW.TEST_SELLER_TYPE_DIM.json", //
                "/table/EDW.TEST_SITES.json", //
                "/table/SSB.CUSTOMER.json", //
                "/table/SSB.DATES.json", //
                "/table/SSB.PART.json", //
                "/table/SSB.SUPPLIER.json", //
                "/table/SSB.V_LINEORDER.json", //
                "/table/DEFAULT.STREAMING_V2_TABLE.json", //
                "/table/DEFAULT.STREAMING_V2_USER_INFO_TABLE.json"
        );
        Set<String> expectTblExd = Sets.newHashSet(
                "/table_exd/DEFAULT.TEST_COUNTRY.json", //
                "/table_exd/DEFAULT.TEST_KYLIN_FACT--default.json"); //
        Assert.assertEquals(expectTbl, tables);
        Assert.assertEquals(expectTblExd, tableExds);
    }
}