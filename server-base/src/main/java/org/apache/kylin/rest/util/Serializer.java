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

package org.apache.kylin.rest.util;

import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class Serializer<T> {
    private final Class<T> type;

    public Serializer(Class<T> type) {
        this.type = type;
    }

    public T deserialize(byte[] value) throws JsonParseException, JsonMappingException, IOException {
        if (null == value) {
            return null;
        }

        return JsonUtil.readValue(value, type);
    }

    public byte[] serialize(T obj) throws JsonProcessingException {
        if (null == obj) {
            return null;
        }

        return JsonUtil.writeValueAsBytes(obj);
    }
}
