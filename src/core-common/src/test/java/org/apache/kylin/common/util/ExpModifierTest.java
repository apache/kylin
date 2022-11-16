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

package org.apache.kylin.common.util;

import org.junit.Assert;
import org.junit.Test;

public class ExpModifierTest {

    @Test
    public void test() throws ParseException {
        ok("current_time", "current_time()");
        ok("current_timestamp", "current_timestamp()");
        ok("current_date", "current_date()");
        ok("'current_date'", "'current_date'");
        ok("\"current_date\"", "\"current_date\"");
        ok("l_current_date", "l_current_date");

        ok("date_sub(current_date, 1)", "date_sub(current_date(), 1)");
        ok("date_sub(current_date(), 1)", "date_sub(current_date(), 1)");
        ok("date_sub(current_date(),1)", "date_sub(current_date(),1)");
    }

    public void ok(String exp, String expected) throws ParseException {
        ExpModifier modifier = new ExpModifier(exp);
        String real = modifier.transform();
        Assert.assertEquals(expected, real);
    }
}
