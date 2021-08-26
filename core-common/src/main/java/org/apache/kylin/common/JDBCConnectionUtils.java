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

package org.apache.kylin.common;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class JDBCConnectionUtils {
    public static final String APPEND_PARAMS = "allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false";
    public static final String HOST_NAME_WHITE_LIST = "[^-._a-zA-Z0-9]";
    public static final String PORT_WHITE_LIST = "[^0-9]";
    public static final String DATABASE_WHITE_LIST = "[^-._a-zA-Z0-9]";
    public static final String PROPERTIES_VALUE_WHITE_LIST = "[^a-zA-Z0-9]";
    public static final String ALLOWED_PROPERTIES = KylinConfig.getInstanceFromEnv().getJdbcUrlAllowedProperties();
    public static final String MYSQL_PREFIX = "jdbc:mysql";
    public static final String SQLSERVER_SCHEMA = KylinConfig.getInstanceFromEnv().getJdbcAllowedSqlServerSchema();
    public static final String ALLOWED_SCHEMA = KylinConfig.getInstanceFromEnv().getJdbcAllowedSchema();
    public static final String H2_MEM_PREFIX = "jdbc:h2:mem";
    public static final String SERVER_INFO_FLAG = "//";
    public static final String DATABASE_FLAG = "/";
    public static final String PROPERTIES_FLAG = "?";
    public static final String PROPERTIES_SEPARATOR = "&";
    public static final String SQLSERVER_SEPARATOR = ";";

    /*
    Mysql jdbc connection url: jdbc:mysql://host:port/database
    Postgresql jdbc connection url: jdbc:postgresql://host:port/database
    Presto jdbc connection url: jdbc:presto://host:port/database
    SQLServer jdbc connection url: jdbc:sqlserver://host:port;property=value;property=value
    H2 jdbc connection url [For test]: jdbc:h2:mem:database
     */
    public static String checkUrl(String url) {
        String[] urlInfo = StringUtil.split(url, ":");
        String schema = null;
        if (urlInfo.length == 4) {
            schema = urlInfo[1];
        } else if (urlInfo.length == 5) {
            schema = urlInfo[1] + ":" + urlInfo[2];
        } else {
            throw new IllegalArgumentException("JDBC URL format does not conform to the specification, Please check it.");
        }

        checkJdbcSchema(schema);

        if (url.startsWith(H2_MEM_PREFIX)) {
            checkIllegalCharacter(urlInfo[3], DATABASE_WHITE_LIST);
            return url;
        }

        String properties = null;
        int databaseSeparatorIndex = url.lastIndexOf(DATABASE_FLAG);
        if (SQLSERVER_SCHEMA.contains(schema)) {
            databaseSeparatorIndex = url.indexOf(SQLSERVER_SEPARATOR);
            properties = url.substring(databaseSeparatorIndex + 1);
            checkJdbcProperties(properties, SQLSERVER_SEPARATOR);
        }

        String[] hostInfo = url.substring(url.indexOf(SERVER_INFO_FLAG) + 2, databaseSeparatorIndex).split(":");
        checkIllegalCharacter(hostInfo[0], HOST_NAME_WHITE_LIST);
        checkIllegalCharacter(hostInfo[1], PORT_WHITE_LIST);

        String database = url.substring(databaseSeparatorIndex + 1);
        int propertiesSeparatorIndex = url.indexOf(PROPERTIES_FLAG);
        if (propertiesSeparatorIndex != -1) {
            database = url.substring(databaseSeparatorIndex + 1, propertiesSeparatorIndex);
            properties = url.substring(propertiesSeparatorIndex + 1);
            checkJdbcProperties(properties, PROPERTIES_SEPARATOR);
        }
        if (!SQLSERVER_SCHEMA.contains(schema)) {
            checkIllegalCharacter(database, DATABASE_WHITE_LIST);
        }
        if (url.startsWith(MYSQL_PREFIX)) {
            if (!url.contains(PROPERTIES_FLAG)) {
                url = url.concat(PROPERTIES_FLAG);
            } else {
                url = url.concat(PROPERTIES_SEPARATOR);
            }
            url = url.concat(APPEND_PARAMS);
        }
        return url;
    }

    private static void checkIllegalCharacter(String info, String whiteList) {
        String repaired = info.replaceAll(whiteList, "");
        if (repaired.length() != info.length()) {
            throw new IllegalArgumentException("Detected illegal character in " + info + " by "
                    + whiteList + ", jdbc url not allowed.");
        }
    }

    private static void checkJdbcSchema(String jdbcSchema) {
        if (!ALLOWED_SCHEMA.contains(jdbcSchema)) {
            throw new IllegalArgumentException("The data source schema : " + jdbcSchema + " is not allowed. " +
                    "You can add the schema to the allowed schema list by kylin.jdbc.url.allowed.additional.schema " +
                    "and separate with commas.");
        }
    }

    private static void checkJdbcProperties(String properties, String separator) {
        if (properties != null) {
            String[] propertiesInfo = StringUtil.split(properties, separator);
            for (String property : propertiesInfo) {
                if (property.isEmpty()) {
                    continue;
                }
                String[] propertyInfo = StringUtil.split(property, "=");
                if (propertyInfo.length < 2) {
                    throw new IllegalArgumentException("Illegal jdbc properties: " + property);
                }
                if (separator.equals(SQLSERVER_SEPARATOR) && "database".equals(propertyInfo[0])) {
                    checkIllegalCharacter(propertyInfo[1], DATABASE_WHITE_LIST);
                    continue;
                }
                Preconditions.checkArgument(
                        ALLOWED_PROPERTIES.contains(propertyInfo[0]),
                        "The property [%s] is not in the allowed list %s, you can add the property to " +
                                "the allowed properties list by kylin.jdbc.url.allowed.properties" +
                                " and separate with commas.",
                        propertyInfo[0],
                        ALLOWED_PROPERTIES
                );
                checkIllegalCharacter(propertyInfo[1], PROPERTIES_VALUE_WHITE_LIST);
            }
        }
    }
}
