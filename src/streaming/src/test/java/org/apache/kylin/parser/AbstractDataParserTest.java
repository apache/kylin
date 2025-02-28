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

import static org.apache.kylin.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.core.JsonParseException;

import lombok.val;

public class AbstractDataParserTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetDataParser() throws Exception {
        String json1 = "{\"cust_no\":\"343242\",\"windowDate\":\"2021-06-01 00:00:00\","
                + "\"windowDateLong\":\"1625037465002\",\"msg_type\":\"single\",\"msg_type1\":\"old\","
                + "\"msg_type2\":\"jily\",\"msg_type3\":\"pandora\",\"age\":\"32\",\"bal1\":21,\"bal2\":12,\"bal3\":13,\"bal4\":15,\"bal5\":22}";
        String json2 = "{\"cust_no\":\"343242\",\"windowDate\":\"2021-06-01 00:00:00\","
                + "\"windowDateLong\":\"1625037465002\",\"msg_type\":\"single\",\"msg_type1\":\"old\","
                + "\"msg_type2\":\"jily\",\"msg_type3\":\"pandora\",\"age\":\"32\",\"bal1\":21,\"bal2\":12,\"bal3\":13,\"bal4\":15,\"bal5\":22";
        val dataParser = AbstractDataParser.getDataParser(DEFAULT_PARSER_NAME,
                Thread.currentThread().getContextClassLoader());
        Assert.assertNotNull(dataParser);
        dataParser.before();
        dataParser.after(Maps.newHashMap());
        Assert.assertTrue(dataParser.defineDataTypes().isEmpty());
        dataParser.process(StandardCharsets.UTF_8.encode(json1));
        dataParser.process(null);
        ByteBuffer encode = StandardCharsets.UTF_8.encode(json2);
        Assert.assertThrows(JsonParseException.class, () -> dataParser.process(encode));

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertThrows(IllegalStateException.class,
                () -> AbstractDataParser.getDataParser(null, contextClassLoader));
        Assert.assertThrows(IllegalStateException.class,
                () -> AbstractDataParser.getDataParser("java.lang.String", contextClassLoader));
    }
}
