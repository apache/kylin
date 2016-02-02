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

//package org.apache.kylin.common.util;
//
//import com.google.common.collect.Iterables;
//import com.google.common.collect.Lists;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.filter.CompareFilter;
//import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
//import org.apache.kylin.common.KylinConfig;
//import org.apache.kylin.common.persistence.HBaseConnection;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.is;
//
///**
// * MockHTable Test.
// *
// * compare to real HTable.
// */
//
//public class ITMockHTableTest extends HBaseMetadataTestCase {
//    private final Logger logger = LoggerFactory.getLogger(this.getClass());
//
//    private static HTableInterface mock;
//    private static HTableInterface real;
//    private static String emailCF = "EMAIL";
//    private static String addressCF = "ADDRESS";
//    private static String emailCQ = "email";
//    private static String addressCQ = "address";
//    private static String tableName = "MockHTable-TEST";
//
//    @BeforeClass
//    public static void setUp() throws Exception {
//
//        staticCreateTestMetadata();
//
//        HBaseConnection.deleteTable(KylinConfig.getInstanceFromEnv().getStorageUrl(), tableName);
//        HBaseConnection.createHTableIfNeeded(KylinConfig.getInstanceFromEnv().getStorageUrl(), tableName, emailCF, addressCF);
//        real = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl()).getTable(tableName);
//        mock = new MockHTable(tableName, emailCF, addressCF);
//
//        setupDefaultData();
//    }
//
//    @AfterClass
//    public static void tearDown() throws Exception {
//        HBaseConnection.deleteTable(KylinConfig.getInstanceFromEnv().getStorageUrl(), tableName);
//        staticCleanupTestMetadata();
//    }
//
//    private static void setupDefaultData() throws IOException, InterruptedException {
//        Put john = createPutForPerson("John Doe", "john.doe@email.com", "US");
//        Put jane = createPutForPerson("Jane Doe", "jane.doe@other.email.com", "US");
//        Put me = createPutForPerson("Yunsang Choi", "oddpoet@gmail", "jeju");
//        mutateAll(john, jane, me);
//    }
//
//    private static Put createPutForPerson(String name, String email, String address) {
//        Put person = new Put(name.getBytes());
//        person.add(emailCF.getBytes(), emailCQ.getBytes(), email.getBytes());
//        person.add(addressCF.getBytes(), addressCQ.getBytes(), address.getBytes());
//        return person;
//    }
//
//    private static <R extends Row> void mutateAll(R... rows) throws IOException, InterruptedException, IOException {
//        real.batch(Lists.newArrayList(rows));
//        mock.batch(Lists.newArrayList(rows));
//    }
//
//    @Test
//    public void test_get() throws Exception {
//        Get get = new Get("John Doe".getBytes());
//
//        Result realResult = real.get(get);
//        Result mockResult = mock.get(get);
//
//        assertThat(realResult.isEmpty(), is(false));
//        assertThat(mockResult.isEmpty(), is(realResult.isEmpty()));
//    }
//
//    @Test
//    public void test_get_with_filter() throws Exception {
//        Get get = new Get("John Doe".getBytes());
//        get.setFilter(new SingleColumnValueFilter(emailCF.getBytes(), emailCQ.getBytes(), CompareFilter.CompareOp.EQUAL, "WRONG EMAIL".getBytes()
//
//        ));
//
//        Result realResult = real.get(get);
//        Result mockResult = mock.get(get);
//
//        assertThat(realResult.isEmpty(), is(true));
//        assertThat(mockResult.isEmpty(), is(realResult.isEmpty()));
//    }
//
//    @Test
//    public void test_exists() throws IOException {
//        Get get = new Get("John Doe".getBytes());
//        boolean realResult = real.exists(get);
//        boolean mockResult = mock.exists(get);
//
//        assertThat(realResult, is(true));
//        assertThat(realResult, is(mockResult));
//    }
//
//    @Test
//    public void test_exists_include_not_exist_column() throws IOException {
//        Get get = new Get("John Doe".getBytes());
//        get.addColumn(emailCF.getBytes(), emailCQ.getBytes());
//        get.addColumn(emailCF.getBytes(), "NOT_EXIST_COLUMN".getBytes());
//        boolean realResult = real.exists(get);
//        boolean mockResult = mock.exists(get);
//
//        assertThat(realResult, is(true));
//        assertThat(realResult, is(mockResult));
//    }
//
//    @Test
//    public void test_exists_with_only_not_exist_column() throws IOException {
//        Get get = new Get("John Doe".getBytes());
//        get.addColumn(emailCF.getBytes(), "NOT_EXIST_COLUMN".getBytes());
//        boolean realResult = real.exists(get);
//        boolean mockResult = mock.exists(get);
//
//        assertThat(realResult, is(false));
//        assertThat(realResult, is(mockResult));
//    }
//
//    @Test
//    public void test_scan_with_filter() throws Exception {
//        Scan scan = new Scan();
//        scan.setFilter(new SingleColumnValueFilter(addressCF.getBytes(), addressCQ.getBytes(), CompareFilter.CompareOp.EQUAL, "US".getBytes()));
//
//        ResultScanner realResult = real.getScanner(scan);
//        ResultScanner mockResult = mock.getScanner(scan);
//
//        logger.debug("mock : {}", mockResult);
//
//        assertThat(Iterables.size(realResult), is(2));
//        assertThat(Iterables.size(mockResult), is(2));
//    }
//
//    @Test
//    public void test_scan_for_pre_match() throws Exception {
//        Scan scan = new Scan("J".getBytes(), "K".getBytes()); // start with 'J' only
//
//        ResultScanner realResult = real.getScanner(scan);
//        ResultScanner mockResult = mock.getScanner(scan);
//
//        assertThat(Iterables.size(realResult), is(2));
//        assertThat(Iterables.size(mockResult), is(2));
//    }
//}