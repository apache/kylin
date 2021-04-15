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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KylinClientTest {

    KylinConnectionInfo connInfo = new KylinConnectionInfo() {
        @Override
        public String getProject() {
            return "default";
        }

        @Override
        public String getBaseUrl() {
            return "http://localhost:7070";
        }

        @Override
        public Properties getConnectionProperties() {
            Properties props = new Properties();
            props.setProperty("user", "ADMIN");
            props.setProperty("password", "KYLIN");
            return props;
        }
    };

    KylinClient client = new KylinClient(connInfo);

    HttpClient httpClient = mock(HttpClient.class);

    @Before
    public void setUp() throws Exception {
        client.setHttpClient(httpClient);
    }

    @Test
    public void connect() throws IOException {
        HttpResponse response = mock(HttpResponse.class);
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(response);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HTTP_1_1, 200, "OK"));
        client.connect();
    }

    @Test
    public void retrieveMetaData() throws IOException {
        HttpResponse response = TestUtil.mockHttpResponseWithFile(200, "OK", "tables_and_columns.json");
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(response);

        KylinMeta.KMetaProject metaData = client.retrieveMetaData(connInfo.getProject());

        assertEquals(connInfo.getProject(), metaData.projectName);
        assertTrue(!metaData.catalogs.isEmpty());
        KylinMeta.KMetaCatalog catalog = metaData.catalogs.get(0);
        assertEquals("defaultCatalog", catalog.getName());
        assertEquals(1, catalog.schemas.size());
        KylinMeta.KMetaSchema schema = catalog.schemas.get(0);
        assertEquals("DEFAULT", schema.getName());
        assertEquals(5, schema.tables.size());
    }

    @Test(expected = AssertionError.class)
    public void retrieveMetaDataWithWrongProject() throws IOException {
        client.retrieveMetaData("defualt2");
    }

    @Test
    public void executeQuery() throws IOException {
        HttpResponse response = TestUtil.mockHttpResponseWithFile(200, "OK", "query.json");
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(response);
        IRemoteClient.QueryResult queryResult = client.executeQuery("SELECT 1 as val", Collections.emptyList(), new HashMap<String, String>());
        assertEquals(1, queryResult.columnMeta.size());
        Iterable<Object> iterable = queryResult.iterable;
        ArrayList<Object> list = Lists.newArrayList(iterable);
        assertEquals(1, list.size());
    }

  @Test(expected = IllegalArgumentException.class)
  public void testWrapObjectThrowsIllegalArgumentExceptionUsingDateType() {
      KylinClient.wrapObject("OQ? PYC6BWm`kOE", Types.DATE);
  }


  @Test
  public void testWrapObjectUsingNull() {
      assertNull(KylinClient.wrapObject(null, 1));
  }


  @Test
  public void testConvertBooleanType() {
      assertEquals("java.lang.Boolean", KylinClient.convertType(Types.BOOLEAN).getName());
  }

}
