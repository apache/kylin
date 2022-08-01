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

import org.apache.kylin.common.util.DateFormat;
import org.junit.Assert;
import org.junit.Test;

public class DateTimeParserTest extends AbstractTimeParserTestBase {
    private static final String TS_PATTERN = "tsPattern";
    private static final String TS_PARSER = "org.apache.kylin.parser.DateTimeParser";

    @Test
    public void testParse_COMPACT_DATE_PATTERN() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.COMPACT_DATE_PATTERN);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("20190923");
        Assert.assertEquals(1569168000000L, time);
    }

    @Test
    public void testParse_DEFAULT_DATE_PATTERN() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATE_PATTERN);
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATE_PATTERN);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("2019-09-23");
        Assert.assertEquals(1569168000000L, time);
    }

    @Test
    public void testParse_DEFAULT_TIME_PATTERN() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_TIME_PATTERN);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("12:13:14");
        Assert.assertEquals(15194000L, time);
    }

    @Test
    public void testParse_DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("2019-09-23 12:13:14");
        Assert.assertEquals(1569211994000L, time);
    }

    @Test
    public void testParse_DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS);
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("2019-09-23 12:13:14.135");
        Assert.assertEquals(1569211994135L, time);
    }

    @Test
    public void testParse_SelfDefined_1() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, "MM-dd-yyyy HH:mm:ss");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("09-23-2019 12:13:14");
        Assert.assertEquals(1569211994000L, time);
    }

    @Test
    public void testParse_SelfDefined_2() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(TS_PATTERN, "HH:mm:ss MM-dd-yyyy");
        AbstractTimeParser timeParser = getDateTimeParser(TS_PARSER, props);
        long time = timeParser.parseTime("12:13:14 09-23-2019");
        Assert.assertEquals(1569211994000L, time);
    }

    @Test
    public void testParseTime() {
        Map<String, String> props1 = new HashMap<>();
        props1.put("tsPattern", "yyyy/MM/dd");
        Map<String, String> props2 = new HashMap<>();
        props2.put("tsPattern", "MM.dd.yyyy");
        DateTimeParser parser1 = new DateTimeParser(props1);
        DateTimeParser parser2 = new DateTimeParser(props2);

        Long parsedTime1 = parser1.parseTime("2000/12/1");
        Long parsedTime2 = parser2.parseTime("12.1.2000");

        Assert.assertEquals(parsedTime1, parsedTime2);
    }
}
