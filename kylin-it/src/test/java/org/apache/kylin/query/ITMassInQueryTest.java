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

package org.apache.kylin.query;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.ITable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 */
public class ITMassInQueryTest extends KylinTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ITMassInQueryTest.class);

    FileSystem fileSystem;
    Set<Long> vipSellers;

    @BeforeClass
    public static void setUp() throws SQLException {
    }

    @AfterClass
    public static void tearDown() {
    }

    @Before
    public void setup() throws Exception {

        ITKylinQueryTest.clean();
        ITKylinQueryTest.joinType = "left";
        ITKylinQueryTest.setupAll();

        fileSystem = HadoopUtil.getWorkingFileSystem();

        int sellerCount = 200;
        Random r = new Random();
        vipSellers = Sets.newHashSet();
        for (int i = 0; i < sellerCount; i++) {
            vipSellers.add(10000000L + r.nextInt(1500));
        }

        Path path = new Path("/tmp/vip_customers.txt");
        fileSystem.delete(path, false);
        FSDataOutputStream outputStream = fileSystem.create(path);
        org.apache.commons.io.IOUtils.write(StringUtils.join(vipSellers, "\n"), outputStream, Charset.defaultCharset());
        outputStream.close();

        System.out.println("The filter is " + vipSellers);
    }

    @After
    public void after() throws Exception {
        ITKylinQueryTest.clean();
    }

    @Test
    @Ignore
    public void testMassInQuery() throws Exception {
        compare("src/test/resources/query/sql_massin", null, true);
    }

    @Test
    @Ignore
    public void testMassInWithDistinctCount() throws Exception {
        run("src/test/resources/query/sql_massin_distinct", null, true);
    }

    protected void run(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        logger.info("---------- test folder: " + queryFolder);
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql = getTextFromFile(sqlFile);

            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, needSort);
            printResult(kylinTable);

        }
    }

    protected void compare(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        logger.info("---------- test folder: " + queryFolder);
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql = getTextFromFile(sqlFile);

            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, needSort);

            // execute H2
            sql = sql.replace("massin(test_kylin_fact.SELLER_ID,'vip_customers')", "test_kylin_fact.SELLER_ID in ( " + org.apache.commons.lang.StringUtils.join(vipSellers, ",") + ")");
            logger.info("Query Result from H2 - " + queryName);
            logger.info("Query for H2 - " + sql);
            ITable h2Table = executeQuery(newH2Connection(), queryName, sql, needSort);

            try {
                // compare the result
                assertTableEquals(h2Table, kylinTable);
            } catch (Throwable t) {
                logger.info("execAndCompQuery failed on: " + sqlFile.getAbsolutePath());
                throw t;
            }
        }
    }

}
