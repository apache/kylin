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

package org.apache.kylin.jdbc.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;

import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.ColumnMetaData.Rep;

import org.apache.kylin.jdbc.stub.KylinColumnMetaData;

/**
 * Util class to handle type gap between sql types and java types.
 * 
 * @author xduo
 * 
 */
public class SQLTypeMap {

    public final static Map<String, ColumnMetaData> schemaMetaTypeMapping = new LinkedHashMap<String, ColumnMetaData>();

    public final static Map<String, ColumnMetaData> columnMetaTypeMapping = new LinkedHashMap<String, ColumnMetaData>();

    public final static Map<String, ColumnMetaData> tableMetaTypeMapping = new LinkedHashMap<String, ColumnMetaData>();

    static {
        schemaMetaTypeMapping.put("TABLE_CAT", KylinColumnMetaData.dummy(0, "TABLE_SCHEM", "TABLE_SCHEM", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        schemaMetaTypeMapping.put("TABLE_SCHEM", KylinColumnMetaData.dummy(1, "TABLE_CATALOG", "TABLE_CATALOG", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));

        tableMetaTypeMapping.put("TABLE_CAT", KylinColumnMetaData.dummy(0, "TABLE_CAT", "TABLE_CAT", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("TABLE_SCHEM", KylinColumnMetaData.dummy(1, "TABLE_SCHEM", "TABLE_SCHEM", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("TABLE_NAME", KylinColumnMetaData.dummy(2, "TABLE_NAME", "TABLE_NAME", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("TABLE_TYPE", KylinColumnMetaData.dummy(3, "TABLE_TYPE", "TABLE_TYPE", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("REMARKS", KylinColumnMetaData.dummy(4, "REMARKS", "REMARKS", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("TYPE_CAT", KylinColumnMetaData.dummy(5, "TYPE_CAT", "TYPE_CAT", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("TYPE_SCHEM", KylinColumnMetaData.dummy(6, "TYPE_SCHEM", "TYPE_SCHEM", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("TYPE_NAME", KylinColumnMetaData.dummy(7, "TYPE_NAME", "TYPE_NAME", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("SELF_REFERENCING_COL_NAME", KylinColumnMetaData.dummy(8, "SELF_REFERENCING_COL_NAME", "SELF_REFERENCING_COL_NAME", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        tableMetaTypeMapping.put("REF_GENERATION", KylinColumnMetaData.dummy(9, "REF_GENERATION", "REF_GENERATION", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));

        columnMetaTypeMapping.put("TABLE_CAT", KylinColumnMetaData.dummy(0, "TABLE_CAT", "TABLE_CAT", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("TABLE_SCHEM", KylinColumnMetaData.dummy(1, "TABLE_SCHEM", "TABLE_SCHEM", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("TABLE_NAME", KylinColumnMetaData.dummy(2, "TABLE_NAME", "TABLE_NAME", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("COLUMN_NAME", KylinColumnMetaData.dummy(3, "COLUMN_NAME", "COLUMN_NAME", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("DATA_TYPE", KylinColumnMetaData.dummy(4, "DATA_TYPE", "DATA_TYPE", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("TYPE_NAME", KylinColumnMetaData.dummy(5, "TYPE_NAME", "TYPE_NAME", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("COLUMN_SIZE", KylinColumnMetaData.dummy(6, "COLUMN_SIZE", "COLUMN_SIZE", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("BUFFER_LENGTH", KylinColumnMetaData.dummy(7, "BUFFER_LENGTH", "BUFFER_LENGTH", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("DECIMAL_DIGITS", KylinColumnMetaData.dummy(8, "DECIMAL_DIGITS", "DECIMAL_DIGITS", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("NUM_PREC_RADIX", KylinColumnMetaData.dummy(9, "NUM_PREC_RADIX", "NUM_PREC_RADIX", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("NULLABLE", KylinColumnMetaData.dummy(10, "NULLABLE", "NULLABLE", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("REMARKS", KylinColumnMetaData.dummy(11, "REMARKS", "REMARKS", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("COLUMN_DEF", KylinColumnMetaData.dummy(12, "COLUMN_DEF", "COLUMN_DEF", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("SQL_DATA_TYPE", KylinColumnMetaData.dummy(13, "SQL_DATA_TYPE", "SQL_DATA_TYPE", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("SQL_DATETIME_SUB", KylinColumnMetaData.dummy(14, "SQL_DATETIME_SUB", "SQL_DATETIME_SUB", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("CHAR_OCTET_LENGTH", KylinColumnMetaData.dummy(15, "CHAR_OCTET_LENGTH", "CHAR_OCTET_LENGTH", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("ORDINAL_POSITION", KylinColumnMetaData.dummy(16, "ORDINAL_POSITION", "ORDINAL_POSITION", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("IS_NULLABLE", KylinColumnMetaData.dummy(17, "IS_NULLABLE", "IS_NULLABLE", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("SCOPE_CATALOG", KylinColumnMetaData.dummy(18, "SCOPE_CATALOG", "SCOPE_CATALOG", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("SCOPE_TABLE", KylinColumnMetaData.dummy(19, "SCOPE_TABLE", "SCOPE_TABLE", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("SOURCE_DATA_TYPE", KylinColumnMetaData.dummy(20, "SOURCE_DATA_TYPE", "SOURCE_DATA_TYPE", ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER), true));
        columnMetaTypeMapping.put("IS_AUTOINCREMENT", KylinColumnMetaData.dummy(21, "IS_AUTOINCREMENT", "IS_AUTOINCREMENT", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        columnMetaTypeMapping.put("IS_GENERATEDCOLUMN", KylinColumnMetaData.dummy(22, "IS_GENERATEDCOLUMN", "IS_GENERATEDCOLUMN", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
    }

    @SuppressWarnings("rawtypes")
    public static Class convert(int sqlType) {
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
        }

        return value;
    }
}
