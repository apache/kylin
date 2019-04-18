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
package org.apache.kylin.stream.source.kafka;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.source.MessageParserInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TimedJsonStreamParserTest extends LocalFileMetadataTestCase {

    private static String[] userNeedColNames;
    private static ObjectMapper mapper;
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class),
            SimpleType.construct(Object.class));

    private static final String jsonFilePath = "src/test/resources/message.json";

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
        mapper = new ObjectMapper();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testNormalValue() throws Exception {
        userNeedColNames = new String[] { "create_at", "id", "is_truncated", "text" };
        TimedJsonStreamParser parser = new TimedJsonStreamParser(mockupTblColRefList(), messageParserInfo());
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);

        ConsumerRecord record = new ConsumerRecord<byte[], byte[]>("", 0, 0, null, buffer.array());
        StreamingMessage parsedMsg = parser.parse(record);
        List<String> values = parsedMsg.getData();
        assertEquals("Jul 20, 2016 9:59:17 AM", values.get(0));
        assertEquals("755703618762862600", values.get(1));
        assertEquals("false", values.get(2));
        assertEquals("dejamos", values.get(3));
    }

    @Test
    public void testEmbeddedValue() throws Exception {
        userNeedColNames = new String[] { "user_id", "user_description", "user_is_PROtected",
                "user_is_Default_Profile_image", "user_type_type_name", "user_type_type1_name" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, messageParserInfo());
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        ConsumerRecord record = new ConsumerRecord<byte[], byte[]>("", 0, 0, null, buffer.array());
        StreamingMessage parsedMsg = parser.parse(record);
        List<String> values = parsedMsg.getData();
        assertNotEquals("", parsedMsg.getData().get(0));
        assertEquals("false", values.get(2));
        assertEquals("vip", values.get(4));
        parsedMsg = parser.parse(record);
        values = parsedMsg.getData();
        assertEquals("4853763947", values.get(0));
        assertEquals("Noticias", values.get(1));
        assertEquals("false", values.get(2));
        assertEquals("false", values.get(3));
        assertEquals("vip", values.get(4));
        assertEquals(StringUtils.EMPTY, values.get(5));
    }

    @Test
    public void testEmbeddedValueFaultTolerant() throws Exception {
        userNeedColNames = new String[] { "user_id", "user_sex" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, messageParserInfo());
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        ConsumerRecord record = new ConsumerRecord<byte[], byte[]>("", 0, 0, null, buffer.array());
        StreamingMessage parsedMsg = parser.parse(record);
        List<String> values = parsedMsg.getData();
        assertEquals("4853763947", values.get(0));
        assertEquals(StringUtils.EMPTY, values.get(1));
    }

    @Test
    public void testEmbeddedArray() throws Exception {
        userNeedColNames = new String[] { "user_departments" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, messageParserInfo());
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        ConsumerRecord record = new ConsumerRecord<byte[], byte[]>("", 0, 0, null, buffer.array());
        StreamingMessage parsedMsg = parser.parse(record);
        List<String> values = parsedMsg.getData();
        assertEquals("[QA, RD, PM]", values.get(0));
    }

    private static MessageParserInfo messageParserInfo() {
        MessageParserInfo parserInfo = new MessageParserInfo();
        parserInfo.setFormatTs(false);
        parserInfo.setTsColName("tS");
        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("user_departments", "user.departments");
        fieldMapping.put("user_id", "user.id");
        fieldMapping.put("user_sex", "user.sex");
        fieldMapping.put("user_description", "user.description");
        fieldMapping.put("user_is_PROtected", "user.is_PROtected");
        fieldMapping.put("user_is_Default_Profile_image", "user.is_Default_Profile_image");
        fieldMapping.put("user_type_type_name", "user.type.type_name");
        fieldMapping.put("user_type_type1_name", "user.type.type1_name");
        parserInfo.setColumnToSourceFieldMapping(fieldMapping);
        return parserInfo;
    }

    private static ByteBuffer getJsonByteBuffer(Object obj) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(obj);
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        buff.position(0);
        return buff;
    }

    private static List<TblColRef> mockupTblColRefList() {
        TableDesc t = TableDesc.mockup("table_a");
        List<TblColRef> list = new ArrayList<>();
        for (int i = 0; i < userNeedColNames.length; i++) {
            TblColRef c = TblColRef.mockup(t, i, userNeedColNames[i], "string");
            list.add(c);
        }
        return list;
    }
}