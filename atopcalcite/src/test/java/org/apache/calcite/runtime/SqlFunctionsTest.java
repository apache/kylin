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

package org.apache.calcite.runtime;

import static org.apache.calcite.avatica.util.DateTimeUtils.ymdToUnixDate;

import org.junit.Assert;
import org.junit.Test;

public class SqlFunctionsTest {
    @Test
    public void testAddMonth() {
        // normal add
        Assert.assertEquals(ymdToUnixDate(2011, 5, 10), SqlFunctions.addMonths(ymdToUnixDate(2011, 4, 10), 1));
        Assert.assertEquals(ymdToUnixDate(2013, 5, 10), SqlFunctions.addMonths(ymdToUnixDate(2011, 4, 10), 25));

        // special case
        Assert.assertEquals(ymdToUnixDate(2011, 2, 28), SqlFunctions.addMonths(ymdToUnixDate(2011, 1, 31), 1));
        Assert.assertEquals(ymdToUnixDate(2012, 2, 29), SqlFunctions.addMonths(ymdToUnixDate(2012, 1, 31), 1));
        Assert.assertEquals(ymdToUnixDate(2014, 2, 28), SqlFunctions.addMonths(ymdToUnixDate(2012, 3, 31), 23));
    }
}
