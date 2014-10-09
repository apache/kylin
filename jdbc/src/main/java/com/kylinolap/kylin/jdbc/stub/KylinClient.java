/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.kylin.jdbc.stub;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.ColumnMetaData.Rep;
import net.hydromatic.avatica.ColumnMetaData.ScalarType;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kylinolap.kylin.jdbc.KylinConnectionImpl;
import com.kylinolap.kylin.jdbc.KylinEnumerator;
import com.kylinolap.kylin.jdbc.KylinJdbc41Factory.KylinJdbc41PreparedStatement;
import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaCatalog;
import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaColumn;
import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaSchema;
import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaTable;
import com.kylinolap.kylin.jdbc.util.DefaultSslProtocolSocketFactory;
import com.kylinolap.kylin.jdbc.util.SQLTypeMap;

/**
 * @author xduo
 * 
 */
public class KylinClient implements RemoteClient {
    private static final Logger logger = LoggerFactory.getLogger(KylinClient.class);

    private final KylinConnectionImpl conn;

    public KylinClient(KylinConnectionImpl conn) {
        this.conn = conn;
    }

    @Override
    public void connect() throws ConnectionException {
        PostMethod post = new PostMethod(conn.getConnectUrl());
        HttpClient httpClient = new HttpClient();

        if (conn.getConnectUrl().toLowerCase().startsWith("https://")) {
            registerSsl();
        }
        addPostHeaders(post);

        try {
            StringRequestEntity requestEntity = new StringRequestEntity("{}", "application/json", "UTF-8");
            post.setRequestEntity(requestEntity);
            httpClient.executeMethod(post);

            if (post.getStatusCode() != 200 && post.getStatusCode() != 201) {
                logger.error("Authentication Failed with error code " + post.getStatusCode()
                        + " and message:\n" + post.getResponseBodyAsString());

                throw new ConnectionException("Authentication Failed.");
            }
        } catch (HttpException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new ConnectionException(e.getLocalizedMessage());
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new ConnectionException(e.getLocalizedMessage());
        }
    }

    @Override
    public MetaProject getMetadata(String project) throws ConnectionException {
        GetMethod get = new GetMethod(conn.getMetaProjectUrl(project));
        HttpClient httpClient = new HttpClient();

        if (conn.getConnectUrl().toLowerCase().startsWith("https://")) {
            registerSsl();
        }
        addPostHeaders(get);

        List<TableMetaStub> tableMetaStubs = null;
        try {
            httpClient.executeMethod(get);

            if (get.getStatusCode() != 200 && get.getStatusCode() != 201) {
                logger.error("Authentication Failed with error code " + get.getStatusCode()
                        + " and message:\n" + get.getResponseBodyAsString());

                throw new ConnectionException("Authentication Failed.");
            }

            tableMetaStubs =
                    new ObjectMapper().readValue(get.getResponseBodyAsString(),
                            new TypeReference<List<TableMetaStub>>() {
                            });

            DataSet<MetaCatalog> metaCatalogs = genMetaCatalogs(tableMetaStubs);
            DataSet<MetaSchema> metaSchemas = genMetaSchemas(tableMetaStubs);
            DataSet<MetaTable> metaTables = genMetaTables(tableMetaStubs);
            DataSet<MetaColumn> metaColumns = genMetaColumns(tableMetaStubs);

            return new MetaProject(metaCatalogs, metaSchemas, metaTables, metaColumns);
        } catch (HttpException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new ConnectionException(e.getLocalizedMessage());
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new ConnectionException(e.getLocalizedMessage());
        }
    }

    @Override
    public DataSet<Object[]> query(AvaticaStatement statement, String sql) throws SQLException {
        SQLResponseStub queryRes = null;

        List<StateParam> params = null;
        if (statement instanceof KylinJdbc41PreparedStatement) {
            params = genPrestateStates(statement);
        }

        queryRes = runKylinQuery(sql, params);

        List<ColumnMetaData> metas = genColumnMeta(queryRes);
        List<Object[]> data = genResultData(queryRes, metas);

        return new DataSet<Object[]>(metas, new KylinEnumerator<Object[]>(data.iterator()));
    }

    private DataSet<MetaCatalog> genMetaCatalogs(List<TableMetaStub> tableMetaStubs) {
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        meta.add(KylinColumnMetaData.dummy(0, "TABLE_CAT", "TABLE_CAT",
                ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));

        Set<MetaCatalog> catalogs = new HashSet<MetaCatalog>();
        for (TableMetaStub tableStub : tableMetaStubs) {
            catalogs.add(new MetaCatalog(tableStub.getTABLE_CAT()));
        }

        return new DataSet<MetaCatalog>(meta, new KylinEnumerator<MetaCatalog>(catalogs.iterator()));
    }

    /**
     * @param tableMetaStubs
     * @return
     */
    private DataSet<MetaSchema> genMetaSchemas(List<TableMetaStub> tableMetaStubs) {
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        for (ColumnMetaData cmd : SQLTypeMap.schemaMetaTypeMapping.values()) {
            meta.add(cmd);
        }

        Set<MetaSchema> schemas = new HashSet<MetaSchema>();
        for (TableMetaStub tableStub : tableMetaStubs) {
            schemas.add(new MetaSchema(tableStub.getTABLE_CAT(), tableStub.getTABLE_SCHEM()));
        }

        return new DataSet<MetaSchema>(meta, new KylinEnumerator<MetaSchema>(schemas.iterator()));
    }

    /**
     * @param tableMetaStub
     * @return
     */
    private DataSet<MetaTable> genMetaTables(List<TableMetaStub> tableMetaStubs) {
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();

        for (ColumnMetaData cmd : SQLTypeMap.tableMetaTypeMapping.values()) {
            meta.add(cmd);
        }

        List<MetaTable> tables = new ArrayList<MetaTable>();
        for (TableMetaStub tableStub : tableMetaStubs) {
            MetaTable table =
                    new MetaTable(tableStub.getTABLE_CAT(), tableStub.getTABLE_SCHEM(),
                            tableStub.getTABLE_NAME(), tableStub.getTABLE_TYPE(), tableStub.getREMARKS(),
                            tableStub.getTYPE_CAT(), tableStub.getTYPE_SCHEM(), tableStub.getTYPE_NAME(),
                            tableStub.getSELF_REFERENCING_COL_NAME(), tableStub.getREF_GENERATION());
            tables.add(table);
        }

        return new DataSet<MetaTable>(meta, new KylinEnumerator<MetaTable>(tables.iterator()));
    }

    /**
     * @param tableMetaStub
     * @return
     */
    private DataSet<MetaColumn> genMetaColumns(List<TableMetaStub> tableMetaStubs) {
        List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();

        for (ColumnMetaData cmd : SQLTypeMap.columnMetaTypeMapping.values()) {
            meta.add(cmd);
        }

        List<MetaColumn> columns = new ArrayList<MetaColumn>();

        for (TableMetaStub tableStub : tableMetaStubs) {
            for (TableMetaStub.ColumnMetaStub columnMetaStub : tableStub.getColumns()) {
                MetaColumn column =
                        new MetaColumn(columnMetaStub.getTABLE_CAT(), columnMetaStub.getTABLE_SCHEM(),
                                columnMetaStub.getTABLE_NAME(), columnMetaStub.getCOLUMN_NAME(),
                                columnMetaStub.getDATA_TYPE(), columnMetaStub.getTYPE_NAME(),
                                columnMetaStub.getCOLUMN_SIZE(), columnMetaStub.getBUFFER_LENGTH(),
                                columnMetaStub.getDECIMAL_DIGITS(), columnMetaStub.getNUM_PREC_RADIX(),
                                columnMetaStub.getNULLABLE(), columnMetaStub.getREMARKS(),
                                columnMetaStub.getCOLUMN_DEF(), columnMetaStub.getSQL_DATA_TYPE(),
                                columnMetaStub.getSQL_DATETIME_SUB(), columnMetaStub.getCHAR_OCTET_LENGTH(),
                                columnMetaStub.getORDINAL_POSITION(), columnMetaStub.getIS_NULLABLE(),
                                columnMetaStub.getSCOPE_CATLOG(), columnMetaStub.getSCOPE_TABLE(),
                                columnMetaStub.getSOURCE_DATA_TYPE(), columnMetaStub.getIS_AUTOINCREMENT(),
                                columnMetaStub.getSCOPE_SCHEMA());

                columns.add(column);
            }
        }

        return new DataSet<MetaColumn>(meta, new KylinEnumerator<MetaColumn>(columns.iterator()));
    }

    /**
     * @param queryRes
     * @param metas
     * @return
     */
    private List<Object[]> genResultData(SQLResponseStub queryRes, List<ColumnMetaData> metas) {
        List<Object[]> data = new ArrayList<Object[]>();
        for (String[] result : queryRes.getResults()) {
            Object[] row = new Object[result.length];

            for (int i = 0; i < result.length; i++) {
                ColumnMetaData meta = metas.get(i);
                row[i] = SQLTypeMap.wrapObject(result[i], meta.type.type);
            }

            data.add(row);
        }
        return data;
    }

    /**
     * @param statement
     * @param params
     */
    private List<StateParam> genPrestateStates(AvaticaStatement statement) {
        List<StateParam> params = new ArrayList<StateParam>();
        List<Object> values = ((KylinJdbc41PreparedStatement) statement).getParameterValues();

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            params.add(new StateParam(value.getClass().getCanonicalName(), (null == value) ? null : String
                    .valueOf(value)));
        }

        return params;
    }

    /**
     * @param queryRes
     * @return
     */
    private List<ColumnMetaData> genColumnMeta(SQLResponseStub queryRes) {
        List<ColumnMetaData> metas = new ArrayList<ColumnMetaData>();
        for (int i = 0; i < queryRes.getColumnMetas().size(); i++) {
            SQLResponseStub.ColumnMetaStub scm = queryRes.getColumnMetas().get(i);
            ScalarType type =
                    ColumnMetaData.scalar(scm.getColumnType(), scm.getColumnTypeName(),
                            Rep.of(SQLTypeMap.convert(scm.getColumnType())));

            ColumnMetaData meta =
                    new ColumnMetaData(i, scm.isAutoIncrement(), scm.isCaseSensitive(), scm.isSearchable(),
                            scm.isCurrency(), scm.getIsNullable(), scm.isSigned(), scm.getDisplaySize(),
                            scm.getLabel(), scm.getName(), scm.getSchemaName(), scm.getPrecision(),
                            scm.getScale(), scm.getTableName(), scm.getSchemaName(), type, scm.isReadOnly(),
                            scm.isWritable(), scm.isWritable(), null);

            metas.add(meta);
        }

        return metas;
    }

    /**
     * @param sql
     * @return
     * @throws IOException
     */
    private SQLResponseStub runKylinQuery(String sql, List<StateParam> params) throws SQLException {
        String paramString = null;
        String url = conn.getQueryUrl();

        if (null != params) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                paramString = mapper.writeValueAsString(params);
                paramString = "\"params\": " + paramString;
            } catch (JsonProcessingException e) {
                logger.error(e.getLocalizedMessage(), e);
            }

            url += "/prestate";
        }

        PostMethod post = new PostMethod(url);
        HttpClient httpClient = new HttpClient();

        if (conn.getQueryUrl().toLowerCase().startsWith("https://")) {
            registerSsl();
        }

        addPostHeaders(post);

        String postBody =
                "{\"sql\":\"" + sql + "\", \"project\":\"" + conn.getProject() + "\""
                        + ((null == paramString) ? "" : "," + paramString) + "}";
        String response = null;
        SQLResponseStub queryRes = null;

        try {
            StringRequestEntity requestEntity =
                    new StringRequestEntity(postBody, "application/json", "UTF-8");
            post.setRequestEntity(requestEntity);

            httpClient.executeMethod(post);
            response = post.getResponseBodyAsString();

            if (post.getStatusCode() != 200 && post.getStatusCode() != 201) {
                logger.error("Failed to query", response);
                throw new SQLException(response);
            }

            queryRes = new ObjectMapper().readValue(response, SQLResponseStub.class);

        } catch (HttpException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new SQLException(e.getLocalizedMessage());
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new SQLException(e.getLocalizedMessage());
        }

        return queryRes;
    }

    private void addPostHeaders(HttpMethodBase method) {
        method.addRequestHeader("Accept", "application/json, text/plain, */*");
        method.addRequestHeader("Content-Type", "application/json");
        method.addRequestHeader("Authorization", "Basic " + conn.getBasicAuthHeader());
    }

    private void registerSsl() {
        Protocol.registerProtocol("https", new Protocol("https",
                (ProtocolSocketFactory) new DefaultSslProtocolSocketFactory(), 443));
    }

    public class StateParam {
        private String className;
        private String value;

        public StateParam(String className, String value) {
            super();
            this.className = className;
            this.value = value;
        }

        public String getClassName() {
            return className;
        }

        public void setClazz(String className) {
            this.className = className;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
