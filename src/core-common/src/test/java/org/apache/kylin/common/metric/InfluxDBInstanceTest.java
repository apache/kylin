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
package org.apache.kylin.common.metric;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.kylin.common.metrics.service.InfluxDBInstance;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.shaded.influxdb.okhttp3.Interceptor;
import io.kyligence.kap.shaded.influxdb.okhttp3.MediaType;
import io.kyligence.kap.shaded.influxdb.okhttp3.OkHttpClient;
import io.kyligence.kap.shaded.influxdb.okhttp3.Protocol;
import io.kyligence.kap.shaded.influxdb.okhttp3.Request;
import io.kyligence.kap.shaded.influxdb.okhttp3.Response;
import io.kyligence.kap.shaded.influxdb.okhttp3.ResponseBody;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;

@MetadataInfo(onlyProps = true)
public class InfluxDBInstanceTest {

    private final String SHOW_DATABASES = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"KE_HISTORY\"]]}]}]}\n";

    private InfluxDBInstance influxDBInstance;

    @BeforeEach
    public void setup() throws Exception {
        influxDBInstance = new InfluxDBInstance("KE_HISTORY", "KE_MONITOR_RP", "", "", 1, false);
        influxDBInstance.init();
        influxDBInstance.setInfluxDB(mockInfluxDB());
    }

    @Test
    public void testBasic() {
        final Map<String, String> tags = Maps.newHashMap();
        tags.put("project", "default");
        final Map<String, Object> fields = Maps.newHashMap();
        fields.put("sql", "selct * from test_table");
        influxDBInstance.write("tb_query", tags, fields, 0);

        QueryResult queryResult = influxDBInstance.read("SHOW DATABASES");
        Assert.assertNull(queryResult.getError());
        Assert.assertNotNull(queryResult.getResults());
    }

    private InfluxDB mockInfluxDB() {
        final OkHttpClient.Builder client = new OkHttpClient.Builder();
        client.addInterceptor(new Interceptor() {
            @Override
            public Response intercept(Chain chain) throws IOException {
                final Request request = chain.request();
                final URL url = request.url().url();
                if ("/ping".equals(url.getPath())) {
                    return mockPingSuccess(request);
                }

                if (url.toString().contains("SHOW+DATABASES")) {
                    return mockShowDatabases(request);
                }

                if ("/write".equals(url.getPath())) {
                    return mockWriteSuccess(request);
                }

                return chain.proceed(request);
            }
        });

        return InfluxDBFactory.connect("http://localhost:8086", "root", "root", client);
    }

    private Response mockPingSuccess(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), "")).build();
    }

    private Response mockShowDatabases(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), SHOW_DATABASES)).build();
    }

    private Response mockWriteSuccess(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), "")).build();
    }
}
