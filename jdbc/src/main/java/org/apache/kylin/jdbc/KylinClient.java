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
import java.math.BigDecimal;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.jdbc.KylinMeta.KMetaCatalog;
import org.apache.kylin.jdbc.KylinMeta.KMetaColumn;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;
import org.apache.kylin.jdbc.KylinMeta.KMetaSchema;
import org.apache.kylin.jdbc.KylinMeta.KMetaTable;
import org.apache.kylin.jdbc.json.PreparedQueryRequest;
import org.apache.kylin.jdbc.json.QueryRequest;
import org.apache.kylin.jdbc.json.SQLResponseStub;
import org.apache.kylin.jdbc.json.StatementParameter;
import org.apache.kylin.jdbc.json.TableMetaStub;
import org.apache.kylin.jdbc.json.TableMetaStub.ColumnMetaStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KylinClient implements IRemoteClient {

    private static final Logger logger = LoggerFactory.getLogger(KylinClient.class);

    private final KylinConnection conn;
    private final Properties connProps;
    private CloseableHttpClient httpClient;
    private final ObjectMapper jsonMapper;

    public KylinClient(KylinConnection conn) {
        this.conn = conn;
        this.connProps = conn.getConnectionProperties();
        this.httpClient = HttpClients.createDefault();
        this.jsonMapper = new ObjectMapper();

        // trust all certificates
        if (isSSL()) {
            try {
                TrustStrategy acceptingTrustStrategy = new TrustStrategy() {
                    public boolean isTrusted(X509Certificate[] certificate, String type) {
                        return true;
                    }
                };
                SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
                httpClient = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).setSSLContext(sslContext).build();
            } catch (Exception e) {
                throw new RuntimeException("Initialize HTTPS client failed", e);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static Class convertType(int sqlType) {
        Class result = Object.class;

        switch (sqlType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            result = String.class;
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            result = BigDecimal.class;
            break;
        case Types.BIT:
            result = Boolean.class;
            break;
        case Types.TINYINT:
            result = Byte.class;
            break;
        case Types.SMALLINT:
            result = Short.class;
            break;
        case Types.INTEGER:
            result = Integer.class;
            break;
        case Types.BIGINT:
            result = Long.class;
            break;
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            result = Double.class;
            break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            result = Byte[].class;
            break;
        case Types.DATE:
            result = Date.class;
            break;
        case Types.TIME:
            result = Time.class;
            break;
        case Types.TIMESTAMP:
            result = Timestamp.class;
            break;
        default:
            //do nothing
            break;
        }

        return result;
    }

    public static Object wrapObject(String value, int sqlType) {
        if (null == value) {
            return null;
        }

        switch (sqlType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return value;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return new BigDecimal(value);
        case Types.BIT:
            return Boolean.parseBoolean(value);
        case Types.TINYINT:
            return Byte.valueOf(value);
        case Types.SMALLINT:
            return Short.valueOf(value);
        case Types.INTEGER:
            return Integer.parseInt(value);
        case Types.BIGINT:
            return Long.parseLong(value);
        case Types.FLOAT:
            return Float.parseFloat(value);
        case Types.REAL:
        case Types.DOUBLE:
            return Double.parseDouble(value);
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return value.getBytes();
        case Types.DATE:
            return Date.valueOf(value);
        case Types.TIME:
            return Time.valueOf(value);
        case Types.TIMESTAMP:
            return Timestamp.valueOf(value);
        default:
            //do nothing
            break;

        }

        return value;
    }

    private boolean isSSL() {
        return Boolean.parseBoolean(connProps.getProperty("ssl", "false"));
    }

    private String baseUrl() {
        return (isSSL() ? "https://" : "http://") + conn.getBaseUrl();
    }

    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Content-Type", "application/json");

        String username = connProps.getProperty("user");
        String password = connProps.getProperty("password");
        String basicAuth = DatatypeConverter.printBase64Binary((username + ":" + password).getBytes());
        method.addHeader("Authorization", "Basic " + basicAuth);
    }

    @Override
    public void connect() throws IOException {
        HttpPost post = new HttpPost(baseUrl() + "/kylin/api/user/authentication");
        addHttpHeaders(post);
        StringEntity requestEntity = new StringEntity("{}", ContentType.create("application/json", "UTF-8"));
        post.setEntity(requestEntity);

        CloseableHttpResponse response = httpClient.execute(post);

        if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
            throw asIOException(post, response);
        }
        response.close();
    }

    @Override
    public KMetaProject retrieveMetaData(String project) throws IOException {
        assert conn.getProject().equals(project);

        String url = baseUrl() + "/kylin/api/tables_and_columns?project=" + project;
        HttpGet get = new HttpGet(url);
        addHttpHeaders(get);

        CloseableHttpResponse response = httpClient.execute(get);

        if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
            throw asIOException(get, response);
        }

        List<TableMetaStub> tableMetaStubs = jsonMapper.readValue(response.getEntity().getContent(), new TypeReference<List<TableMetaStub>>() {
        });

        List<KMetaTable> tables = convertMetaTables(tableMetaStubs);
        List<KMetaSchema> schemas = convertMetaSchemas(tables);
        List<KMetaCatalog> catalogs = convertMetaCatalogs(schemas);
        response.close();
        return new KMetaProject(project, catalogs);
    }

    private List<KMetaCatalog> convertMetaCatalogs(List<KMetaSchema> schemas) {
        Map<String, List<KMetaSchema>> catalogMap = new LinkedHashMap<String, List<KMetaSchema>>();
        for (KMetaSchema schema : schemas) {
            List<KMetaSchema> list = catalogMap.get(schema.tableCatalog);
            if (list == null) {
                list = new ArrayList<KMetaSchema>();
                catalogMap.put(schema.tableCatalog, list);
            }
            list.add(schema);
        }

        List<KMetaCatalog> result = new ArrayList<KMetaCatalog>();
        for (List<KMetaSchema> catSchemas : catalogMap.values()) {
            String catalog = catSchemas.get(0).tableCatalog;
            result.add(new KMetaCatalog(catalog, catSchemas));
        }
        return result;
    }

    private List<KMetaSchema> convertMetaSchemas(List<KMetaTable> tables) {
        Map<String, List<KMetaTable>> schemaMap = new LinkedHashMap<String, List<KMetaTable>>();
        for (KMetaTable table : tables) {
            String key = table.tableCat + "!!" + table.tableSchem;
            List<KMetaTable> list = schemaMap.get(key);
            if (list == null) {
                list = new ArrayList<KMetaTable>();
                schemaMap.put(key, list);
            }
            list.add(table);
        }

        List<KMetaSchema> result = new ArrayList<KMetaSchema>();
        for (List<KMetaTable> schemaTables : schemaMap.values()) {
            String catalog = schemaTables.get(0).tableCat;
            String schema = schemaTables.get(0).tableSchem;
            result.add(new KMetaSchema(catalog, schema, schemaTables));
        }
        return result;
    }

    private List<KMetaTable> convertMetaTables(List<TableMetaStub> tableMetaStubs) {
        List<KMetaTable> result = new ArrayList<KMetaTable>(tableMetaStubs.size());
        for (TableMetaStub tableStub : tableMetaStubs) {
            result.add(convertMetaTable(tableStub));
        }
        return result;
    }

    private KMetaTable convertMetaTable(TableMetaStub tableStub) {
        List<KMetaColumn> columns = new ArrayList<KMetaColumn>(tableStub.getColumns().size());
        for (ColumnMetaStub columnStub : tableStub.getColumns()) {
            columns.add(convertMetaColumn(columnStub));
        }
        return new KMetaTable(tableStub.getTABLE_CAT(), tableStub.getTABLE_SCHEM(), tableStub.getTABLE_NAME(), tableStub.getTABLE_TYPE(), columns);
    }

    private KMetaColumn convertMetaColumn(ColumnMetaStub columnStub) {
        return new KMetaColumn(columnStub.getTABLE_CAT(), columnStub.getTABLE_SCHEM(), columnStub.getTABLE_NAME(), columnStub.getCOLUMN_NAME(), columnStub.getDATA_TYPE(), columnStub.getTYPE_NAME(), columnStub.getCOLUMN_SIZE(), columnStub.getDECIMAL_DIGITS(), columnStub.getNUM_PREC_RADIX(), columnStub.getNULLABLE(), columnStub.getCHAR_OCTET_LENGTH(), columnStub.getORDINAL_POSITION(), columnStub.getIS_NULLABLE());
    }

    @Override
    public QueryResult executeQuery(String sql, List<AvaticaParameter> params, List<Object> paramValues) throws IOException {

        SQLResponseStub queryResp = executeKylinQuery(sql, convertParameters(params, paramValues));
        if (queryResp.getIsException())
            throw new IOException(queryResp.getExceptionMessage());

        List<ColumnMetaData> metas = convertColumnMeta(queryResp);
        List<Object> data = convertResultData(queryResp, metas);

        return new QueryResult(metas, data);
    }

    private List<StatementParameter> convertParameters(List<AvaticaParameter> params, List<Object> paramValues) {
        if (params == null || params.isEmpty())
            return null;

        assert params.size() == paramValues.size();

        List<StatementParameter> result = new ArrayList<StatementParameter>();
        for (Object v : paramValues) {
            result.add(new StatementParameter(v.getClass().getCanonicalName(), String.valueOf(v)));
        }
        return result;
    }

    private SQLResponseStub executeKylinQuery(String sql, List<StatementParameter> params) throws IOException {
        String url = baseUrl() + "/kylin/api/query";
        String project = conn.getProject();

        QueryRequest request = null;
        if (null != params) {
            request = new PreparedQueryRequest();
            ((PreparedQueryRequest) request).setParams(params);
            url += "/prestate"; // means prepared statement..
        } else {
            request = new QueryRequest();
        }
        request.setSql(sql);
        request.setProject(project);

        HttpPost post = new HttpPost(url);
        addHttpHeaders(post);

        String postBody = jsonMapper.writeValueAsString(request);
        logger.debug("Post body:\n " + postBody);
        StringEntity requestEntity = new StringEntity(postBody, ContentType.create("application/json", "UTF-8"));
        post.setEntity(requestEntity);

        CloseableHttpResponse response = httpClient.execute(post);

        if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
            throw asIOException(post, response);
        }

        SQLResponseStub stub = jsonMapper.readValue(response.getEntity().getContent(), SQLResponseStub.class);
        response.close();
        return stub;
    }

    private List<ColumnMetaData> convertColumnMeta(SQLResponseStub queryResp) {
        List<ColumnMetaData> metas = new ArrayList<ColumnMetaData>();
        for (int i = 0; i < queryResp.getColumnMetas().size(); i++) {
            SQLResponseStub.ColumnMetaStub scm = queryResp.getColumnMetas().get(i);
            Class columnClass = convertType(scm.getColumnType());
            ScalarType type = ColumnMetaData.scalar(scm.getColumnType(), scm.getColumnTypeName(), Rep.of(columnClass));

            ColumnMetaData meta = new ColumnMetaData(i, scm.isAutoIncrement(), scm.isCaseSensitive(), scm.isSearchable(), scm.isCurrency(), scm.getIsNullable(), scm.isSigned(), scm.getDisplaySize(), scm.getLabel(), scm.getName(), scm.getSchemaName(), scm.getPrecision(), scm.getScale(), scm.getTableName(), scm.getSchemaName(), type, scm.isReadOnly(), scm.isWritable(), scm.isWritable(), columnClass.getCanonicalName());

            metas.add(meta);
        }

        return metas;
    }

    private List<Object> convertResultData(SQLResponseStub queryResp, List<ColumnMetaData> metas) {
        List<String[]> stringResults = queryResp.getResults();
        List<Object> data = new ArrayList<Object>(stringResults.size());
        for (String[] result : stringResults) {
            Object[] row = new Object[result.length];

            for (int i = 0; i < result.length; i++) {
                ColumnMetaData meta = metas.get(i);
                row[i] = wrapObject(result[i], meta.type.id);
            }

            data.add(row);
        }
        return (List<Object>) data;
    }

    private IOException asIOException(HttpRequestBase request, HttpResponse response) throws IOException {
        return new IOException(request.getMethod() + " failed, error code " + response.getStatusLine().getStatusCode() + " and response: " + EntityUtils.toString(response.getEntity()));
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
