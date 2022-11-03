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

import java.io.IOException;
import java.util.HashMap;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.val;

public class JacksonTest {
    @Test
    public void foo() throws IOException {
        HashMap a = new HashMap<String, String>();
        a.put("1", "1");
        a.put("3", "3");
        a.put("2", "2");

        JacksonBean bean = new JacksonBean();
        bean.setA("valuea");
        bean.setConfiguration(a);

        String s = JsonUtil.writeValueAsString(bean);
        System.out.println(s);

        JacksonBean desBean = (JacksonBean) JsonUtil.readValue(
                "{\"a\":\"valuea\",\"b\":0,\"configuration\":{\"2\":\"2\",\"3\":\"3\",\"1\":\"1\"}}",
                JacksonBean.class);

        String x2 = JsonUtil.writeValueAsString(desBean);
        System.out.println(x2);

        System.out.println(desBean);
    }

    @Test
    public void testSerializeWithView() throws JsonProcessingException {
        val e = new TestEntity();
        e.setMvcc(2);
        e.setUuid("123456");
        e.setVersion("1.2.3");
        val str1 = JsonUtil.writeValueAsString(e);
        Assert.assertFalse(str1.contains("mvcc"));

        val mapper = new ObjectMapper();
        val str2 = mapper.writeValueAsString(e);
        Assert.assertTrue(str2.contains("mvcc"));
    }

    public static class TestEntity extends RootPersistentEntity {
    }
}
