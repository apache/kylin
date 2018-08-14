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

package org.apache.kylin.jdbc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.mockito.Mockito;

/**
 * For test purpose.
 */
public final class TestUtil {

    private TestUtil() {
    }

    public static String getResourceContent(String path) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        StringWriter writer = new StringWriter();
        InputStream is = loader.getResourceAsStream(path);
        if (is == null) {
            throw new IllegalArgumentException(new FileNotFoundException(path + " not found in class path"));
        }
        try {
            IOUtils.copy(is, writer, "utf-8");
            return writer.toString();
        } catch (IOException e) {
            IOUtils.closeQuietly(is);
            throw new RuntimeException(e);
        }
    }

    public static HttpResponse mockHttpResponse(int statusCode, String message, String body) {
        HttpResponse response = Mockito.mock(HttpResponse.class);
        Mockito.when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, statusCode, message));
        Mockito.when(response.getEntity()).thenReturn(new StringEntity(body, StandardCharsets.UTF_8));
        return response;
    }

    public static HttpResponse mockHttpResponseWithFile(int statusCode, String message, String path) {
        return mockHttpResponse(statusCode, message, getResourceContent(path));
    }
}
