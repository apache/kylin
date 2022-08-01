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

package org.apache.kylin.parser;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class DefaultTimeParserTest extends AbstractTimeParserTestBase {
    private static final String TS_TIMEZONE = "tsTimezone";
    private static final String TS_PARSER = "org.apache.kylin.parser.DefaultTimeParser";

    @Test
    public void testParseBlankTime() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("");
        Assert.assertEquals(0L, time);
        time = timeParser.parseTime(null);
        Assert.assertEquals(0L, time);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNoValidTime() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        timeParser.parseTime("1a11b23423c");
    }

    @Test
    public void testParseTimeWithTimeZone_GMT() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("1569240794000");
        Assert.assertEquals(1569240794000L, time);
    }

    @Test
    public void testParseTimeWithTimeZone_GMT_0() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT+0");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("1569240794000");
        Assert.assertEquals(1569240794000L, time);
    }

    @Test
    public void testParseTimeWithTimeZone_GMT_8() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_TIMEZONE, "GMT+8");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("1569211994000");
        Assert.assertEquals(1569240794000L, time);
    }
}
