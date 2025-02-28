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

import static org.apache.kylin.jdbc.LoggerUtils.entry;
import static org.apache.kylin.jdbc.LoggerUtils.exit;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.jdbc.KylinMeta.KMetaCatalog;
import org.apache.kylin.jdbc.KylinMeta.KMetaColumn;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;
import org.apache.kylin.jdbc.KylinMeta.KMetaSchema;
import org.apache.kylin.jdbc.KylinMeta.KMetaTable;
import org.apache.kylin.jdbc.json.GenericResponse;
import org.apache.kylin.jdbc.json.PreparedQueryRequest;
import org.apache.kylin.jdbc.json.SQLResponseStub;
import org.apache.kylin.jdbc.json.StatementParameter;
import org.apache.kylin.jdbc.json.TableMetaStub;
import org.apache.kylin.jdbc.json.TableMetaStub.ColumnMetaStub;
import org.apache.kylin.jdbc.json.TableWithComment;
import org.apache.kylin.jdbc.json.TablesWithCommentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KylinClient implements IRemoteClient {

    // TODO: cannot support tableau

    private static final Logger logger = LoggerFactory.getLogger(KylinClient.class);

    private final KylinConnection conn;
    private final Properties connProps;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper jsonMapper;
    private static final int POOL_MAX = 10;
    private static final int POOL_MIN = 0;
    private static final int RESPONSE_CODE_200 = 200;
    private static final int RESPONSE_CODE_201 = 201;
    private static final String APPLICATION = "application/json";
    private static final String TIME_ZONE = "UTC";
    private static final String AUTH_METHOD = "Basic ";

    public KylinClient(KylinConnection conn) {
        entry(logger);
        this.conn = conn;
        this.connProps = conn.getConnectionProperties();
        this.httpClient = buildHttpClient();
        this.jsonMapper = new ObjectMapper();
        jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        exit(logger);
    }

    private CloseableHttpClient buildHttpClient() {
        HttpClientBuilder builder = HttpClients.custom();

        // network timeout
        int timeout = Integer.parseInt(connProps.getProperty("timeout", "0"));
        RequestConfig rconf = RequestConfig.custom().setConnectTimeout(timeout).setSocketTimeout(timeout).build();
        builder.setDefaultRequestConfig(rconf);
        logger.debug("use connection timeout " + timeout);

        // SSL friendly
        PoolingHttpClientConnectionManager cm;
        if (isSSL()) {
            try {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial((TrustStrategy) (x509Certificates, s) -> true).build();
                SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, (s, sslSession) -> true);
                Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory> create()
                        .register("https", sslsf).build();
                cm = new PoolingHttpClientConnectionManager(r);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            logger.debug("use SSL connection with optimistic trust");
        } else {
            cm = new PoolingHttpClientConnectionManager();
            logger.debug("use non-SSL connection");
        }

        // connection pool
        int pool = Integer.parseInt(connProps.getProperty("pool", "1"));
        if (pool > POOL_MAX || pool < POOL_MIN) {
            logger.debug("invalid 'pool', reset to default");
            pool = 1;
        }
        if (pool == 0) {
            logger.debug("use NO connection pool");
        } else {
            cm.setDefaultMaxPerRoute(pool);
            cm.setMaxTotal(pool);
            logger.debug("use connection pool size " + pool);
        }

        builder.setConnectionManager(cm);
        return builder.build();
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
        case Types.BOOLEAN:
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

    public Object wrapObject(String value, int sqlType) {
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
        case Types.BOOLEAN:
            return Boolean.parseBoolean(value);
        case Types.TINYINT:
            return Byte.parseByte(value);
        case Types.SMALLINT:
            return Short.parseShort(value);
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
            return value.getBytes(Charset.defaultCharset());
        case Types.DATE:
            return dateConverter(value);
        case Types.TIME:
            return timeConverter(value);
        case Types.TIMESTAMP:
            return timestampConverter(value);
        default:
            //do nothing
            break;
        }

        return value;
    }

    private Date dateConverter(String value) {
        try {
            return new Date(parseDateTime(value, "yyyy-MM-dd"));
        } catch (ParseException ex) {
            logger.error("parse date failed!", ex);
            return null;
        }
    }

    private Time timeConverter(String value) {
        try {
            return new Time(parseDateTime(value, "HH:mm:ss"));
        } catch (ParseException ex) {
            logger.error("parse time failed!", ex);
            return null;
        }
    }

    private Timestamp timestampConverter(String value) {
        String[] formats = new String[] { "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss" };
        ParseException ex = null;
        for (String format : formats) {
            try {
                return new Timestamp(parseDateTime(value, format));
            } catch (ParseException e) {
                ex = e;
            }
        }
        logger.error("parse timestamp failed!", ex);
        return null;
    }

    private long parseDateTime(String value, String format) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat(format, Locale.getDefault(Locale.Category.FORMAT));
        formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        return formatter.parse(value).getTime();
    }

    private boolean isSSL() {
        return Boolean.parseBoolean(connProps.getProperty("ssl", "false"));
    }

    private String baseUrl() {
        return (isSSL() ? "https://" : "http://") + conn.getBaseUrl();
    }

    private void addHttpHeadersV2(HttpRequestBase method) {
        String headerVal = "application/vnd.apache.kylin-v4-public+json, text/plain, */*";
        method.addHeader("Accept", headerVal);
        addCommonHttpHeaders(method);
    }

    private void addCommonHttpHeaders(HttpRequestBase method) {
        method.addHeader("Content-Type", APPLICATION);
        method.addHeader("User-Agent", "KylinJDBCDriver");
        String authToken = connProps.getProperty("auth-token");
        if (authToken == null || authToken.trim().isEmpty()) {
            String username = connProps.getProperty("user");
            String password = connProps.getProperty("password");
            authToken = DatatypeConverter
                    .printBase64Binary((username + ":" + password).getBytes(Charset.defaultCharset()));
        }
        method.addHeader("Authorization", AUTH_METHOD + authToken);
        method.addHeader("Auto", "true");
    }

    @Override
    public void connect() throws IOException {
        entry(logger);

        // authorization post
        HttpPost post = new HttpPost(baseUrl() + "/kylin/api/user/authentication");
        addHttpHeadersV2(post);
        StringEntity requestEntity = new StringEntity("{}", ContentType.create(APPLICATION, "UTF-8"));
        post.setEntity(requestEntity);

        try {
            HttpResponse response = httpClient.execute(post);

            if (response.getStatusLine().getStatusCode() != RESPONSE_CODE_200
                    && response.getStatusLine().getStatusCode() != RESPONSE_CODE_201) {
                throw asIOException(post, response);
            }
        } finally {
            post.releaseConnection();
        }
        exit(logger);
    }

    @Override
    public KMetaProject retrieveMetaData(String project) throws IOException {
        entry(logger);
        if (!conn.getProject().equals(project)) {
            throw new IllegalArgumentException(
                    "Project name [" + project + "] not fit current connection[" + conn.getProject() + "]");
        }
        String url = baseUrl() + "/kylin/api/query/tables_and_columns?project=" + project;
        HttpGet get = new HttpGet(url);
        addHttpHeadersV2(get);

        List<TableMetaStub> tableMetaStubs;
        try {
            HttpResponse response = httpClient.execute(get);

            if (response.getStatusLine().getStatusCode() != RESPONSE_CODE_200
                    && response.getStatusLine().getStatusCode() != RESPONSE_CODE_201) {
                throw asIOException(get, response);
            }

            GenericResponse<List<TableMetaStub>> tableMetaStubVPlus = jsonMapper.readValue(
                    response.getEntity().getContent(), new TypeReference<GenericResponse<List<TableMetaStub>>>() {
                    });

            if (tableMetaStubVPlus == null || tableMetaStubVPlus.getData() == null) {
                throw new IOException("Response abnormal without data");
            }

            tableMetaStubs = tableMetaStubVPlus.getData();
        } finally {
            get.releaseConnection();
        }

        String urlWithComment = baseUrl() + "/kylin/api/tables?project=" + project + "&page_size=" + Integer.MAX_VALUE;
        HttpGet getWithComment = new HttpGet(urlWithComment);
        addHttpHeadersV2(getWithComment);
        try {
            HttpResponse responseWithComment = httpClient.execute(getWithComment);
            GenericResponse<TablesWithCommentResponse> tablesWithCommentResponseVPlus = jsonMapper.readValue(
                    responseWithComment.getEntity().getContent(),
                    new TypeReference<GenericResponse<TablesWithCommentResponse>>() {
                    });
            TablesWithCommentResponse tablesWithCommentResponse = tablesWithCommentResponseVPlus.getData();
            List<TableWithComment> tableWithComments = tablesWithCommentResponse.getValue();
            Map<String, String> columnWithCommentMap = new HashMap<>();
            for (TableWithComment tableWithComment : tableWithComments) {
                for (TableWithComment.ColumnWithComment columnWithComment : tableWithComment.getColumns()) {
                    String keyWithComment = tableWithComment.getDatabase() + "." + tableWithComment.getName() + "."
                            + columnWithComment.getName();
                    columnWithCommentMap.put(keyWithComment, columnWithComment.getComment());
                }
            }
            for (TableMetaStub tableMetaStub : tableMetaStubs) {
                for (ColumnMetaStub columnMetaStub : tableMetaStub.getColumns()) {
                    String keyMetaStub = tableMetaStub.getTABLE_SCHEM() + "." + tableMetaStub.getTABLE_NAME() + "."
                            + columnMetaStub.getCOLUMN_NAME();
                    if (columnWithCommentMap.containsKey(keyMetaStub)) {
                        columnMetaStub.setREMARKS(columnWithCommentMap.get(keyMetaStub));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Get comment failed:" + e.getMessage(), e);
        } finally {
            getWithComment.releaseConnection();
        }
        List<KMetaTable> tables = convertMetaTables(tableMetaStubs);
        List<KMetaSchema> schemas = convertMetaSchemas(tables);
        List<KMetaCatalog> catalogs = convertMetaCatalogs(schemas);
        KMetaProject metaProject = new KMetaProject(project, catalogs);
        exit(logger);
        return metaProject;
    }

    private List<KMetaCatalog> convertMetaCatalogs(List<KMetaSchema> schemas) {
        Map<String, List<KMetaSchema>> catalogMap = new LinkedHashMap<>();
        for (KMetaSchema schema : schemas) {
            List<KMetaSchema> list = catalogMap.computeIfAbsent(schema.tableCatalog, k -> new ArrayList<>());
            list.add(schema);
        }

        List<KMetaCatalog> result = new ArrayList<>();
        for (List<KMetaSchema> catSchemas : catalogMap.values()) {
            String catalog = catSchemas.get(0).tableCatalog;
            result.add(new KMetaCatalog(catalog, catSchemas));
        }
        return result;
    }

    private List<KMetaSchema> convertMetaSchemas(List<KMetaTable> tables) {
        Map<String, List<KMetaTable>> schemaMap = new LinkedHashMap<>();
        for (KMetaTable table : tables) {
            String key = table.tableCat + "!!" + table.tableSchem;
            List<KMetaTable> list = schemaMap.computeIfAbsent(key, k -> new ArrayList<>());
            list.add(table);
        }

        List<KMetaSchema> result = new ArrayList<>();
        for (List<KMetaTable> schemaTables : schemaMap.values()) {
            String catalog = schemaTables.get(0).tableCat;
            String schema = schemaTables.get(0).tableSchem;
            result.add(new KMetaSchema(catalog, schema, schemaTables));
        }
        return result;
    }

    private List<KMetaTable> convertMetaTables(List<TableMetaStub> tableMetaStubs) {
        List<KMetaTable> result = new ArrayList<>(tableMetaStubs.size());
        for (TableMetaStub tableStub : tableMetaStubs) {
            result.add(convertMetaTable(tableStub));
        }
        return result;
    }

    private KMetaTable convertMetaTable(TableMetaStub tableStub) {
        List<KMetaColumn> columns = new ArrayList<>(tableStub.getColumns().size());
        for (ColumnMetaStub columnStub : tableStub.getColumns()) {
            columns.add(convertMetaColumn(columnStub));
        }
        return new KMetaTable(tableStub.getTABLE_CAT(), tableStub.getTABLE_SCHEM(), tableStub.getTABLE_NAME(),
                tableStub.getTABLE_TYPE(), columns);
    }

    private KMetaColumn convertMetaColumn(ColumnMetaStub columnStub) {
        return new KMetaColumn(columnStub.getTABLE_CAT(), columnStub.getTABLE_SCHEM(), columnStub.getTABLE_NAME(),
                columnStub.getCOLUMN_NAME(), columnStub.getDATA_TYPE(), columnStub.getTYPE_NAME(),
                columnStub.getCOLUMN_SIZE(), columnStub.getDECIMAL_DIGITS(), columnStub.getNUM_PREC_RADIX(),
                columnStub.getNULLABLE(), columnStub.getCHAR_OCTET_LENGTH(), columnStub.getORDINAL_POSITION(),
                columnStub.getIS_NULLABLE(), columnStub.getREMARKS());
    }

    @Override
    public QueryResult executeQuery(String sql, List<AvaticaParameter> params, List<Object> paramValues,
            Map<String, String> queryToggles, String queryId) throws IOException {
        entry(logger);
        GenericResponse<SQLResponseStub> queryResp = executeKylinQuery(sql, convertParameters(params, paramValues),
                queryToggles, queryId);
        if (logger.isDebugEnabled()) {
            logger.debug("Response:\n {} ", jsonMapper.writeValueAsString(queryResp));
        }
        if (queryResp == null || queryResp.getData() == null) {
            throw new IOException("Response abnormal without data");
        }
        SQLResponseStub resp = queryResp.getData();
        if (resp.getIsException()) {
            throw new IOException(resp.getExceptionMessage());
        }

        List<ColumnMetaData> metas = convertColumnMeta(resp);
        List<Object> data = convertResultData(resp, metas);
        QueryResult result = new QueryResult(metas, data, resp.getQueryId(), resp.getDuration(),
                resp.getResultRowCount());
        exit(logger);
        return result;
    }

    private List<StatementParameter> convertParameters(List<AvaticaParameter> params, List<Object> paramValues) {
        List<StatementParameter> result = new ArrayList<>();
        if (params == null || params.isEmpty()) {
            return result;
        }

        if (params.size() != paramValues.size()) {
            throw new IllegalArgumentException(
                    "Param count:" + params.size() + "mismatch values count:" + paramValues.size());
        }

        for (Object v : paramValues) {
            if (v == null) {
                result.add(new StatementParameter(Object.class.getCanonicalName(), null));
            } else {
                result.add(new StatementParameter(v.getClass().getCanonicalName(), String.valueOf(v)));
            }
        }
        return result;
    }

    public GenericResponse<SQLResponseStub> executeKylinQuery(String sql, List<StatementParameter> params,
            Map<String, String> queryToggles, String queryId) throws IOException {
        long start = System.currentTimeMillis();

        String url = baseUrl() + "/kylin/api/query";
        String project = conn.getProject();

        PreparedQueryRequest request = new PreparedQueryRequest();
        if (null != params) {
            request.setParams(params);
        }
        request.setQueryId(queryId);
        request.setSql(sql);
        request.setProject(project);
        request.setBackdoorToggles(queryToggles);
        int maxRowsNum = Integer.parseInt(queryToggles.get("ATTR_STATEMENT_MAX_ROWS"));
        if (maxRowsNum > 0) {
            request.setLimit(maxRowsNum);
        }

        String executeAs = connProps.getProperty("EXECUTE_AS_USER_ID");
        if (executeAs != null) {
            request.setExecuteAs(executeAs);
            String formatString = "Found the parameter EXECUTE_AS_USER_ID in the connection string. "
                    + "The query will be executed as the user defined in this connection string.\n"
                    + "EXECUTE_AS_USER_ID = {}";
            logger.info(formatString, executeAs);
        }

        HttpPost post = new HttpPost(url);
        addHttpHeadersV2(post);

        String postBody = jsonMapper.writeValueAsString(request);
        logger.info("Post body:\n {} ", postBody);
        StringEntity requestEntity = new StringEntity(postBody, ContentType.create(APPLICATION, "UTF-8"));
        post.setEntity(requestEntity);

        try (CloseableHttpResponse response = httpClient.execute(post)) {

            if (response.getStatusLine().getStatusCode() != RESPONSE_CODE_200
                    && response.getStatusLine().getStatusCode() != RESPONSE_CODE_201) {
                throw asIOException(post, response);
            }

            GenericResponse<SQLResponseStub> r = jsonMapper.readValue(response.getEntity().getContent(),
                    new TypeReference<GenericResponse<SQLResponseStub>>() {
                    });

            long dur = System.currentTimeMillis() - start;
            SQLResponseStub rr = r.getData();
            if (maxRowsNum > 0 && rr.getResults().size() > maxRowsNum) {
                rr.setResults(rr.getResults().subList(0, maxRowsNum));
            }
            logger.info("Query " + rr.getQueryId() + " returned "
                    + (rr.getIsException() ? r.getCode() + "+ex" : r.getCode()) + " in " + dur + " millis at client, "
                    + rr.getDuration() + " millis at server");
            return r;
        }
    }

    private List<ColumnMetaData> convertColumnMeta(SQLResponseStub queryResp) {
        List<ColumnMetaData> metas = new ArrayList<>();
        for (int i = 0; i < queryResp.getColumnMetas().size(); i++) {
            SQLResponseStub.ColumnMetaStub scm = queryResp.getColumnMetas().get(i);
            Class<?> columnClass = convertType(scm.getColumnType());
            int columnType = scm.getColumnType();
            String columnTypeName = scm.getColumnTypeName();
            if (columnClass == BigDecimal.class) {
                columnClass = Number.class;
            } else if (columnType == -1) {
                columnType = 12;
            }
            ScalarType type = ColumnMetaData.scalar(columnType, columnTypeName, Rep.of(columnClass));

            ColumnMetaData meta = new ColumnMetaData(i, scm.isAutoIncrement(), scm.isCaseSensitive(),
                    scm.isSearchable(), scm.isCurrency(), scm.getIsNullable(), scm.isSigned(), scm.getDisplaySize(),
                    scm.getLabel(), scm.getName(), scm.getSchemaName(), scm.getPrecision(), scm.getScale(),
                    scm.getTableName(), scm.getSchemaName(), type, scm.isReadOnly(), scm.isWritable(), scm.isWritable(),
                    columnClass.getCanonicalName());

            metas.add(meta);
        }

        return metas;
    }

    private List<Object> convertResultData(SQLResponseStub queryResp, List<ColumnMetaData> metas) {
        List<String[]> stringResults = queryResp.getResults();
        List<Object> data = new ArrayList<>(stringResults.size());
        for (String[] result : stringResults) {
            Object[] row = new Object[result.length];

            for (int i = 0; i < result.length; i++) {
                ColumnMetaData meta = metas.get(i);
                row[i] = wrapObject(result[i], meta.type.id);
            }

            data.add(row);
        }
        return data;
    }

    private IOException asIOException(HttpRequestBase request, HttpResponse response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        String responseStr = EntityUtils.toString(response.getEntity());
        if (responseStr.contains("Error occured while trying to proxy to")) {
            return new IOException("FAILED!\n"
                    + "[Kylin 5][JDBCDriver]  Unsupported Apache Kylin instance, please contact Apache Kylin Community.");
        } else {
            return new IOException(
                    request.getMethod() + " failed, error code " + statusCode + " and response: " + responseStr);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
