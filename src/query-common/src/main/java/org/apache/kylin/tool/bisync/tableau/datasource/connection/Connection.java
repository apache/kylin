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
package org.apache.kylin.tool.bisync.tableau.datasource.connection;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Connection {

    @JacksonXmlProperty(localName = "class", isAttribute = true)
    private String className;

    @JacksonXmlProperty(localName = "dbname", isAttribute = true)
    private String dbName;

    @JacksonXmlProperty(localName = "odbc-connect-string-extras", isAttribute = true)
    private String odbcConnectStringExtras;

    @JacksonXmlProperty(localName = "odbc-dbms-name", isAttribute = true)
    private String odbcDbmsName;

    @JacksonXmlProperty(localName = "odbc-driver", isAttribute = true)
    private String odbcDriver;

    @JacksonXmlProperty(localName = "odbc-dsn", isAttribute = true)
    private String odbcDsn;

    @JacksonXmlProperty(localName = "odbc-suppress-connection-pooling", isAttribute = true)
    private String odbcSuppressConnectionPooling;

    @JacksonXmlProperty(localName = "odbc-use-connection-pooling", isAttribute = true)
    private String odbcUseConnectionPooling;

    @JacksonXmlProperty(localName = "port", isAttribute = true)
    private String port;

    @JacksonXmlProperty(localName = "schema", isAttribute = true)
    private String schema;

    @JacksonXmlProperty(localName = "server", isAttribute = true)
    private String server;

    @JacksonXmlProperty(localName = "username", isAttribute = true)
    private String userName;

    @JacksonXmlProperty(localName = "connection-customization", isAttribute = true)
    private ConnectionCustomization connectionCustomization;

    public void setOdbcConnectStringExtras(String odbcConnectStringExtras) {
        this.odbcConnectStringExtras = odbcConnectStringExtras;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
