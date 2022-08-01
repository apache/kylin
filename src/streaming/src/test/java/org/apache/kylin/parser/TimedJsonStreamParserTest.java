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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.streaming.metadata.StreamingMessageRow;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;

public class TimedJsonStreamParserTest extends NLocalFileMetadataTestCase {

    private static final String jsonFilePath = "src/test/resources/message.json";
    private static final String dupKeyJsonFilePath = "src/test/resources/message_with_dup_key.json";
    private static String[] userNeedColNames;
    private static String[] userNeedColNamesComment;
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
        staticCleanupTestMetadata();
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
            TblColRef c = TblColRef.mockup(t, i, userNeedColNames[i], "string", comments[i], null);
            list.add(c);
        }
        return list;
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
    public void testFlattenMessage() throws Exception {
        userNeedColNames = new String[] { "user_id", "user_description", "user_isProtected",
                "user_is_Default_Profile_Image" };
        userNeedColNamesComment = new String[] { "", "", "",
                "user" + TimedJsonStreamParser.EMBEDDED_PROPERTY_SEPARATOR + "is_Default_Profile_Image" };
        InputStream is = new FileInputStream(new File(jsonFilePath));
        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.toByteArray(is));
        TimedJsonStreamParser parser = new TimedJsonStreamParser(null, null);
        Map<String, Object> flatMap = parser.flattenMessage(buffer);
        assertEquals(29, flatMap.size());
        assertEquals("Jul 20, 2016 9:59:17 AM", flatMap.get("createdAt"));
        assertEquals(755703618762862600L, flatMap.get("id"));
        assertEquals(false, flatMap.get("isTruncated"));
        assertEquals("dejamos", flatMap.get("text"));
        assertEquals("", flatMap.get("contributorsIDs"));
        assertEquals(755703584084328400L, flatMap.get("mediaEntities[0]_id"));
        assertEquals(150, flatMap.get("mediaEntities[0]_sizes_0_width"));
        assertEquals(100, flatMap.get("mediaEntities[0]_sizes_1_resize"));
        assertEquals(4853763947L, flatMap.get("user_id"));
        assertEquals("Noticias", flatMap.get("user_description"));
        assertEquals(false, flatMap.get("user_is_Default_Profile_Image"));
        assertEquals(false, flatMap.get("user_isProtected"));

    }

    @Test
    public void testFlattenMessageWithDupKey() throws Exception {
        InputStream is = new FileInputStream(new File(dupKeyJsonFilePath));
        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.toByteArray(is));
        TimedJsonStreamParser parser = new TimedJsonStreamParser(null, null);
        Map<String, Object> flatMap = parser.flattenMessage(buffer);
        assertEquals(31, flatMap.size());
        assertEquals("Jul 20, 2016 9:59:17 AM", flatMap.get("createdAt"));
        assertEquals(755703618762862600L, flatMap.get("id"));
        assertEquals(false, flatMap.get("isTruncated"));
        assertEquals("dejamos", flatMap.get("text"));
        assertEquals("", flatMap.get("contributorsIDs"));
        assertEquals(755703584084328400L, flatMap.get("mediaEntities[0]_id"));
        assertEquals(150, flatMap.get("mediaEntities[0]_sizes_0_width"));
        assertEquals(100, flatMap.get("mediaEntities[0]_sizes_1_resize"));
        assertEquals("Noticias", flatMap.get("user_description"));
        assertEquals(false, flatMap.get("user_is_Default_Profile_Image"));
        assertEquals(false, flatMap.get("user_isProtected"));

        // assert dup key val
        assertEquals(123456, flatMap.get("user_id"));
        assertEquals(4853763947L, flatMap.get("user_id_1"));
        assertEquals(654321, flatMap.get("user_id_1_1"));
    }

    @Test
    public void testParseMessageWithColumnMapping() throws Exception {
        userNeedColNames = new String[] { "ID", "DESCRIPTION", "PROTECTED", "PROFILE_IMAGE" };
        userNeedColNamesComment = new String[] { "", "", "",
                "user" + TimedJsonStreamParser.EMBEDDED_PROPERTY_SEPARATOR + "is_Default_Profile_Image" };
        Map<String, String> columnMapping = new HashMap<>();
        columnMapping.put("ID", "user_id");
        columnMapping.put("DESCRIPTION", "user_description");
        columnMapping.put("PROTECTED", "user_isProtected");
        columnMapping.put("PROFILE_IMAGE", "user_is_Default_Profile_Image");

        List<TblColRef> allCol = mockupTblColRefListWithComment(userNeedColNamesComment);
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        parser.setColumnMapping(columnMapping);
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

    /**
     * Not used currently
     */
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
        Assert.assertNotNull(result);

    }

    /**
     * Not used currently
     */
    @Test
    public void testMapValue() throws Exception {
        userNeedColNames = new String[] { "user" };
        List<TblColRef> allCol = mockupTblColRefList();
        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();
        Assert.assertNotNull(result);
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

    @Test
    public void testTimeZoneValue() throws Exception {
        userNeedColNames = new String[] { "order_time" };
        List<TblColRef> allCol = mockupTblColRefList();
        Object msg = mapper.readValue(new File(jsonFilePath), mapType);
        ByteBuffer buffer = getJsonByteBuffer(msg);

        TimedJsonStreamParser parser = new TimedJsonStreamParser(allCol, null);
        List<StreamingMessageRow> msgList = parser.parse(buffer);
        List<String> result = msgList.get(0).getData();
        assertEquals("1559735677057", result.get(0));

        Map<String, String> properties = StreamingParser.defaultProperties;
        properties.put("tsColName", "order_time");
        ByteBuffer buffer1 = getJsonByteBuffer(msg);
        TimedJsonStreamParser parser1 = new TimedJsonStreamParser(allCol, properties);
        List<StreamingMessageRow> msgList1 = parser1.parse(buffer1);
        List<String> result1 = msgList1.get(0).getData();
        assertEquals("2019-06-05 19:54:37", result1.get(0));
    }
}
