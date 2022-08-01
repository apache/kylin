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
package org.apache.kylin.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.session.web.http.CookieSerializer;

@MetadataInfo(onlyProps = true)
class BootstrapServerTest {

    @InjectMocks
    BootstrapServer server = Mockito.spy(new BootstrapServer());

    @Test
    void testCookieSerializer() throws Exception {
        String metadataUrl = "test_docker@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://mysql:3306/kylin?useSSL=false,createDatabaseIfNotExist=true,username=root,password=root";
        String expectedName = "ce8a5bd5b23fe84d1894dce1bd9399364ba5af3c7a25f025442c92244c729804";

        KylinConfig.getInstanceFromEnv().setMetadataUrl(metadataUrl);
        CookieSerializer cookieSerializer = server.cookieSerializer();

        Class cookieClass = cookieSerializer.getClass();
        Field field = cookieClass.getDeclaredField("cookieName");
        field.setAccessible(true);
        Object cookieName = field.get(cookieSerializer);

        assertEquals(cookieName, expectedName);
    }

}
