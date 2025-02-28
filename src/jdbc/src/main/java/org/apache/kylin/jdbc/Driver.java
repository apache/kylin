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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.Scanner;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Kylin JDBC Driver based on Calcite Avatica and Kylin restful API.<br>
 * Supported versions:
 * </p>
 * <ul>
 * <li>jdbc 4.0</li>
 * <li>jdbc 4.1</li>
 * </ul>
 *
 * <p>
 * Supported Statements:
 * </p>
 * <ul>
 * <li>{@link KylinStatement}</li>
 * <li>{@link KylinPreparedStatement}</li>
 * </ul>
 *
 * <p>
 * Supported properties:
 * <ul>
 * <li>user: username</li>
 * <li>password: password</li>
 * <li>ssl: true/false</li>
 * </ul>
 * </p>
 *
 * <p>
 * Driver init code sample:<br>
 *
 * <pre>
 * Driver driver = (Driver) Class.forName(&quot;org.apache.kylin.jdbc.Driver&quot;).newInstance();
 * Properties info = new Properties();
 * info.put(&quot;user&quot;, &quot;user&quot;);
 * info.put(&quot;password&quot;, &quot;password&quot;);
 * info.put(&quot;ssl&quot;, true);
 * Connection conn = driver.connect(&quot;jdbc:kylin://{domain}/{project}&quot;, info);
 * </pre>
 *
 * </p>
 */
public class Driver extends UnregisteredDriver {

    private static final Logger logger = LoggerFactory.getLogger(Driver.class);
    public static final String CONNECT_STRING_PREFIX = "jdbc:kylin:";
    public static final String PARAM_SPLIT_REGEX = ";";

    static {
        LogInitializer.init();
        entry(logger);
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Error occurred while registering JDBC driver " + Driver.class.getName() + ": " + e);
        }
        exit(logger);
    }

    private static InputStream preprocessPropertiesFile(String myFile) throws IOException {
        try (Scanner in = new Scanner(new FileReader(myFile))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while (in.hasNext()) {
                out.write(in.nextLine().replace("\\", "\\\\").getBytes(Charset.defaultCharset()));
                out.write("\n".getBytes(Charset.defaultCharset()));
            }
            return new ByteArrayInputStream(out.toByteArray());
        }
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        } else {
            String prefix = this.getConnectStringPrefix();

            if (!url.startsWith(prefix)) {
                throw new AssertionError();
            }

            Properties info2 = connectStringParseProperties(url, info);
            AvaticaConnection connection = this.factory.newConnection(this, this.factory, url, info2);
            this.handler.onConnectionInit(connection);
            return connection;
        }
    }

    private Properties connectStringParseProperties(String url, Properties props) {
        Properties newProps;
        if (props == null) {
            newProps = new Properties();
        } else {
            newProps = (Properties) props.clone();
        }

        String prefixParams = url.split("//")[0].substring(this.getConnectStringPrefix().length());
        propertyParser(prefixParams, newProps);

        URI uri = URI.create(url.substring(CONNECT_STRING_PREFIX.length()));
        String propString = uri.getRawQuery();
        propertyParser(propString, newProps);

        return newProps;
    }

    private void propertyParser(String propString, Properties props) {
        if (propString == null) {
            return;
        }

        for (String propPair : propString.split(PARAM_SPLIT_REGEX)) {
            if (!propPair.contains("=")) {
                continue;
            }

            String[] splitted = propPair.split("=");
            String key = urlDecoder(splitted[0]);
            String val = "";
            if (splitted.length > 2) {
                IllegalArgumentException e = new IllegalArgumentException(String.format(Locale.ROOT,
                        "The paramter in connection string '%s' is in wrong format." + " Please correct it and retry.",
                        propPair));
                logger.error(e.getMessage());
                throw e;
            }
            if (splitted.length == 2) {
                val = urlDecoder(splitted[1]);
            }
            props.put(key, val);
        }
    }

    private String urlDecoder(String url) {
        try {
            url = java.net.URLDecoder.decode(url, "utf-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("Url decode fail: ", e);
        }
        return url;
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(Driver.class, "org-apache-kylin-jdbc.properties", "Kylin JDBC Driver",
                "unknown version", "Kylin", "unknown version");
    }

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        switch (jdbcVersion) {
        case JDBC_30:
            throw new UnsupportedOperationException();
        case JDBC_40:
            return KylinJdbcFactory.Version40.class.getName();
        case JDBC_41:
        default:
            return KylinJdbcFactory.Version41.class.getName();
        }
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
        entry(logger);
        Meta meta = new KylinMeta((KylinConnection) connection);
        exit(logger);
        return meta;
    }
}
