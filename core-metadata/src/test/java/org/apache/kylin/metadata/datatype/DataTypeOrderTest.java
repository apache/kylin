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

package org.apache.kylin.metadata.datatype;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class DataTypeOrderTest {
    @Test
    public void testDataTypeOrder() {
        DataType intType = DataType.getType("integer");
        DataTypeOrder dataTypeOrder = intType.getOrder();
        Set<String> integers = Sets.newHashSet("100000", "2", "1000", "100", "77", "10", "9", "2000000", "-10000", "0");
        Assert.assertEquals("2000000", dataTypeOrder.max(integers));
        Assert.assertEquals("-10000", dataTypeOrder.min(integers));

        DataType doubleType = DataType.getType("double");
        dataTypeOrder = doubleType.getOrder();
        Set<String> doubels = Sets.newHashSet("1.1", "-299.5", "100000", "1.000", "4.000000001", "0.00", "-1000000.231231", "8000000",
                "10", "10.00");
        Assert.assertEquals("8000000", dataTypeOrder.max(doubels));
        Assert.assertEquals("-1000000.231231", dataTypeOrder.min(doubels));

        DataType datetimeType = DataType.getType("date");
        dataTypeOrder = datetimeType.getOrder();
        Set<String> datetimes = Sets.newHashSet("2010-01-02", "2888-08-09", "2018-05-26", "1527512082000", "2010-02-03 23:59:59",
                "2000-12-12 12:00:00", "1970-01-19 00:18:32", "1998-12-02", "2018-05-28 10:00:00.255", "1995-09-20 20:00:00.220");
        Assert.assertEquals("2888-08-09", dataTypeOrder.max(datetimes));
        Assert.assertEquals("1970-01-19 00:18:32", dataTypeOrder.min(datetimes));

        DataType stringType = new DataType("varchar", 256, 10);
        dataTypeOrder = stringType.getOrder();
        Set<String> strings = Sets.newHashSet(null, "", "中国", "China No.1", "神兽麒麟", "Rocket", "Apache Kylin", "google", "NULL",
                "empty");
        Assert.assertEquals("神兽麒麟", dataTypeOrder.max(strings));
        Assert.assertEquals("", dataTypeOrder.min(strings));
    }
}
