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
package org.apache.kylin.source.kafka;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.StreamingMessageRow;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;

public class TimedJsonStreamParserTest extends LocalFileMetadataTestCase {

    private static String[] userNeedColNames;
    private static String[] userNeedColNamesComment;
    private static final String jsonFilePath = "src/test/resources/message.json";
    private static ObjectMapper mapper;
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class),
            SimpleType.construct(Object.class));

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
        userNeedColNames = new String[] { "createdAt", "id", "isTruncated", "text" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();
        assertEquals("Jul 20, 2016 9:59:17 AM", result.get(0));
        assertEquals("755703618762862600", result.get(1));
        assertEquals("false", result.get(2));
        assertEquals("dejamos", result.get(3));
    }

    @Test
    public void testEmbeddedValue() throws Exception {
        userNeedColNames = new String[] { "user_id", "user_description", "user_isProtected",
                "user_is_Default_Profile_Image" };
        userNeedColNamesComment = new String[] { "", "", "",
                "user" + TimedJsonStreamParser.EMBEDDED_PROPERTY_SEPARATOR + "is_Default_Profile_Image" };
        List<TblColRef> allCol = mockupTblColRefListWithComment(userNeedColNamesComment);
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();
        assertEquals("4853763947", result.get(0));
        assertEquals("Noticias", result.get(1));
        assertEquals("false", result.get(2));
        assertEquals("false", result.get(3));
    }

    @Test
    public void testEmbeddedValueFaultTolerant() throws Exception {
        userNeedColNames = new String[] { "user_id", "nonexisted_description" };
        userNeedColNamesComment = new String[] { "", "" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();
        assertEquals("4853763947", result.get(0));
        assertEquals(StringUtils.EMPTY, result.get(1));
    }

    @Test
    public void testArrayValue() throws Exception {
        userNeedColNames = new String[] { "userMentionEntities", "mediaEntities" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        HashMap<String, Object> map = (HashMap<String, Object>) msg;
        Object array = map.get("mediaEntities");
        ByteBuffer buffer = getJsonByteBuffer(msg);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();
        System.out.println(result);

    }

    @Test
    public void testMapValue() throws Exception {
        userNeedColNames = new String[] { "user" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();

    }

    @Test
    public void testNullKey() throws Exception {
        userNeedColNames = new String[] { "null", "" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();
        assertEquals(StringUtils.EMPTY, result.get(0));
        assertEquals(StringUtils.EMPTY, result.get(1));
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

    private static List<TblColRef> mockupTblColRefListWithComment(String[] comments) {
        TableDesc t = TableDesc.mockup("table_a");
        List<TblColRef> list = new ArrayList<>();
        for (int i = 0; i < userNeedColNames.length; i++) {
            TblColRef c = TblColRef.mockup(t, i, userNeedColNames[i], "string", comments[i]);
            list.add(c);
        }
        return list;
    }
}
