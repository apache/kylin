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

import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;

import static org.apache.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KylinConnectionTest {

    private final Driver driver = new Driver();
    private final KylinJdbcFactory factory = spy(new KylinJdbcFactory.Version41());
    private final IRemoteClient client = mock(IRemoteClient.class);
    private final HttpClient httpClient = mock(HttpClient.class);

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testPrepareStatementWithMockKylinClient() throws SQLException, IOException {
        String sql = "select 1 as val";
        // mock client
        when(client.executeQuery(anyString(), Mockito.<List<Object>>any(), Mockito.<Map<String, String>>any())).thenReturn(getMockResult());

        try (KylinConnection conn = getConnectionWithMockClient()) {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                verify(client).executeQuery(eq(sql), Mockito.<List<Object>>any(), Mockito.<Map<String, String>>any());

                assertTrue(resultSet.next());
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("VAL", metaData.getColumnName(1));
                assertEquals(1, resultSet.getInt("VAL"));
            }
        }
    }

    @Test
    public void testPrepareStatementWithMockHttp() throws IOException, SQLException {
        String sql = "select 1 as val";
        try (KylinConnection connection = getConnectionWithMockHttp()) {

            // mock http
            HttpResponse response = TestUtil.mockHttpResponseWithFile(200, "OK", "query.json");
            when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(response);

            try (ResultSet resultSet = connection.prepareStatement(sql).executeQuery()) {
                assertTrue(resultSet.next());
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("VAL", metaData.getColumnName(1));
                assertEquals(1, resultSet.getInt("VAL"));
            }
        }
    }

    private KylinConnection getConnectionWithMockClient() throws SQLException {
        return getConnectionWithMockClient("jdbc:kylin:test_url/test_db", new Properties());
    }

    private KylinConnection getConnectionWithMockClient(String url, @Nonnull Properties info) throws SQLException {
        info.setProperty("user", "ADMIN");
        info.setProperty("password", "KYLIN");

        doReturn(client).when(factory).newRemoteClient(any(KylinConnectionInfo.class));
        return new KylinConnection(driver, factory, url, info);
    }

    private KylinConnection getConnectionWithMockHttp() throws SQLException, IOException {
        final Properties info = new Properties();
        info.setProperty("user", "ADMIN");
        info.setProperty("password", "KYLIN");

        // hack KylinClient
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invo) throws Throwable {
                KylinConnectionInfo connInfo = invo.getArgument(0);
                KylinClient client = new KylinClient(connInfo);
                client.setHttpClient(httpClient);
                return client;
            }
        }).when(factory).newRemoteClient(any(KylinConnectionInfo.class));

        // Workaround IRemoteClient.connect()
        HttpResponse response = mock(HttpResponse.class);
        when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(response);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HTTP_1_1, 200, "OK"));

        return new KylinConnection(driver, factory, "jdbc:kylin:test_url/test_db", info);
    }

    @Test
    public void testJdbcClientCalcitePropsInUrl() throws Exception {
        String sql = "select 1 as val";

        // mock client
        when(client.executeQuery(anyString(), Mockito.<List<Object>>any(), Mockito.<Map<String, String>>any())).thenReturn(getMockResult());
        Map<String, String> toggles = new HashMap<>();
        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
        info.setProperty("unquotedCasing", "UNCHANGED");
        try (KylinConnection conn = getConnectionWithMockClient("jdbc:kylin:test_url/test_db", info)) {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                verify(client).executeQuery(eq(sql), Mockito.<List<Object>>any(), argThat(new ArgumentMatcher<Map<String, String>>() {
                    @Override
                    public boolean matches(Map<String, String> argument) {
                        String propsStr = argument.get("JDBC_CLIENT_CALCITE_PROPS");
                        assertNotNull(propsStr);
                        Properties props = new Properties();
                        try {
                            props.load(new StringReader(propsStr));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        assertEquals("false", props.getProperty("caseSensitive"));
                        assertEquals("UNCHANGED", props.getProperty("unquotedCasing"));
                        return true;
                    }
                }));

                assertTrue(resultSet.next());
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("VAL", metaData.getColumnName(1));
                assertEquals(1, resultSet.getInt("VAL"));
            }
        }
    }

    private IRemoteClient.QueryResult getMockResult() {
        ArrayList<ColumnMetaData> columnMeta = new ArrayList<>();
        columnMeta.add(new ColumnMetaData(0, false, true, false,
                false, 1, true, 1,
                "VAL", "VAL", null,
                10, 0, null, null,
                ColumnMetaData.scalar(Types.INTEGER, "INTEGER", ColumnMetaData.Rep.INTEGER),
                true, false, false, "java.lang.Integer"));
        ArrayList<Object> list = new ArrayList<>();
        list.add(new Object[]{1});
        return new IRemoteClient.QueryResult(columnMeta, list);
    }
}
