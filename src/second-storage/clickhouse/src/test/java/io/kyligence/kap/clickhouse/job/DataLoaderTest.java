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

package io.kyligence.kap.clickhouse.job;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataLoaderTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void clickHouseType() {
        Assert.assertEquals("UInt8", DataLoader.clickHouseType(DataType.getType(DataType.BOOLEAN)));
        Assert.assertEquals("Int8", DataLoader.clickHouseType(DataType.getType(DataType.BYTE)));

        Assert.assertEquals("Int8", DataLoader.clickHouseType(DataType.getType(DataType.TINY_INT)));

        Assert.assertEquals("Int16", DataLoader.clickHouseType(DataType.getType(DataType.SHORT)));
        Assert.assertEquals("Int16", DataLoader.clickHouseType(DataType.getType(DataType.SMALL_INT)));

        Assert.assertEquals("Int32", DataLoader.clickHouseType(DataType.getType(DataType.INT)));
        Assert.assertEquals("Int32", DataLoader.clickHouseType(DataType.getType(DataType.INT4)));
        Assert.assertEquals("Int32", DataLoader.clickHouseType(DataType.getType(DataType.INTEGER)));

        Assert.assertEquals("Int64", DataLoader.clickHouseType(DataType.getType(DataType.LONG)));
        Assert.assertEquals("Int64", DataLoader.clickHouseType(DataType.getType(DataType.LONG8)));
        Assert.assertEquals("Int64", DataLoader.clickHouseType(DataType.getType(DataType.BIGINT)));

        Assert.assertEquals("Float32", DataLoader.clickHouseType(DataType.getType(DataType.FLOAT)));
        Assert.assertEquals("Float64", DataLoader.clickHouseType(DataType.getType(DataType.DOUBLE)));

        Assert.assertEquals("Decimal(19,4)", DataLoader.clickHouseType(DataType.getType(DataType.DECIMAL)));
        Assert.assertEquals("Decimal(19,4)", DataLoader.clickHouseType(DataType.getType(DataType.NUMERIC)));

        Assert.assertEquals("String", DataLoader.clickHouseType(DataType.getType(DataType.VARCHAR)));
        Assert.assertEquals("String", DataLoader.clickHouseType(DataType.getType(DataType.CHAR)));
        Assert.assertEquals("String", DataLoader.clickHouseType(DataType.getType(DataType.STRING)));

        Assert.assertEquals("Date", DataLoader.clickHouseType(DataType.getType(DataType.DATE)));

        Assert.assertEquals("DateTime", DataLoader.clickHouseType(DataType.getType(DataType.DATETIME)));
        Assert.assertEquals("DateTime", DataLoader.clickHouseType(DataType.getType(DataType.TIMESTAMP)));

    }
}
