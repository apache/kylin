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
package org.apache.kylin.tool.discovery;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ServiceInstanceSerializer<T> extends JsonInstanceSerializer<T> {

    private ObjectMapper mapper = new ObjectMapper();

    public ServiceInstanceSerializer(Class<T> payloadClass) {
        super(payloadClass);
    }

    @Override
    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
        String content = new String(bytes, Charset.defaultCharset());
        Map map = mapper.readValue(content, Map.class);
        return castToServiceInstance(map);
    }

    private ServiceInstance<T> castToServiceInstance(Map map) {
        String name = (String) map.getOrDefault("name", "");
        String id = (String) map.getOrDefault("id", "");
        String address = (String) map.getOrDefault("address", null);
        Integer port = (Integer) map.getOrDefault("port", null);

        return new ServiceInstance(name, id, address, port, null, null, 0L, ServiceType.DYNAMIC, null);
    }
}
