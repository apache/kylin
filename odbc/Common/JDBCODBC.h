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


#pragma once


enum JDBCTypes
{
    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>BIT</code>.
    */
    JDBC_BIT = -7,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>TINYINT</code>.
    */
    JDBC_TINYINT = -6,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>SMALLINT</code>.
    */
    JDBC_SMALLINT = 5,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>INTEGER</code>.
    */
    JDBC_INTEGER = 4,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>BIGINT</code>.
    */
    JDBC_BIGINT = -5,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>FLOAT</code>.
    */
    JDBC_FLOAT = 6,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>REAL</code>.
    */
    JDBC_REAL = 7,


    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>DOUBLE</code>.
    */
    JDBC_DOUBLE = 8,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>NUMERIC</code>.
    */
    JDBC_NUMERIC = 2,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>DECIMAL</code>.
    */
    JDBC_DECIMAL = 3,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>CHAR</code>.
    */
    JDBC_CHAR = 1,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>VARCHAR</code>.
    */
    JDBC_VARCHAR = 12,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>LONGVARCHAR</code>.
    */
    JDBC_LONGVARCHAR = -1,


    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>DATE</code>.
    */
    JDBC_DATE = 91,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>TIME</code>.
    */
    JDBC_TIME = 92,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>TIMESTAMP</code>.
    */
    JDBC_TIMESTAMP = 93,


    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>BINARY</code>.
    */
    JDBC_BINARY = -2,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>VARBINARY</code>.
    */
    JDBC_VARBINARY = -3,

    /**
        <P>The constant in the Java programming language, sometimes referred
        to as a type code, that identifies the generic SQL type
        <code>LONGVARBINARY</code>.
    */
    JDBC_LONGVARBINARY = -4,

    /**
        <P>The constant in the Java programming language
        that identifies the generic SQL value
        <code>NULL</code>.
    */
    //NULL = 0,

    /**
        The constant in the Java programming language that indicates
        that the SQL type is database-specific and
        gets mapped to a Java object that can be accessed via
        the methods <code>getObject</code> and <code>setObject</code>.
    */
    JDBC_OTHER = 1111,


    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type
        <code>JAVA_OBJECT</code>.
        @since 1.2
    */
    JDBC_JAVA_OBJECT = 2000,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type
        <code>DISTINCT</code>.
        @since 1.2
    */
    JDBC_DISTINCT = 2001,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type
        <code>STRUCT</code>.
        @since 1.2
    */
    JDBC_STRUCT = 2002,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type
        <code>ARRAY</code>.
        @since 1.2
    */
    JDBC_ARRAY = 2003,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type
        <code>BLOB</code>.
        @since 1.2
    */
    JDBC_BLOB = 2004,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type
        <code>CLOB</code>.
        @since 1.2
    */
    JDBC_CLOB = 2005,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type
        <code>REF</code>.
        @since 1.2
    */
    JDBC_REF = 2006,

    /**
        The constant in the Java programming language, somtimes referred to
        as a type code, that identifies the generic SQL type <code>DATALINK</code>.
    
        @since 1.4
    */
    JDBC_DATALINK = 70,

    /**
        The constant in the Java programming language, somtimes referred to
        as a type code, that identifies the generic SQL type <code>BOOLEAN</code>.
    
        @since 1.4
    */
    JDBC_BOOLEAN = 16,

    //------------------------- JDBC 4.0 -----------------------------------

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type <code>ROWID</code>
    
        @since 1.6
    
    */
    JDBC_ROWID = -8,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type <code>NCHAR</code>
    
        @since 1.6
    */
    JDBC_NCHAR = -15,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type <code>NVARCHAR</code>.
    
        @since 1.6
    */
    JDBC_NVARCHAR = -9,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type <code>LONGNVARCHAR</code>.
    
        @since 1.6
    */
    JDBC_LONGNVARCHAR = -16,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type <code>NCLOB</code>.
    
        @since 1.6
    */
    JDBC_NCLOB = 2011,

    /**
        The constant in the Java programming language, sometimes referred to
        as a type code, that identifies the generic SQL type <code>XML</code>.
    
        @since 1.6
    */
    JDBC_SQLXML = 2009
};

enum ODBCTypes
{
    // Summary:
    //     Maps to SQL_GUID.
    ODBC_Guid = -11,
    //
    // Summary:
    //     Maps to SQL_WLONGVARCHAR.  Native Type: System.String
    ODBC_WLongVarChar = -10,
    //
    // Summary:
    //     Maps to SQL_WVARCHAR.  Native Type: System.String
    ODBC_WVarChar = -9,
    //
    // Summary:
    //     Maps to SQL_WCHAR.  Native Type: System.String
    ODBC_WChar = -8,
    //
    // Summary:
    //     Maps to SQL_BIT.  Native Type: System.Boolean
    ODBC_Bit = -7,
    //
    // Summary:
    //     Maps to SQL_TINYINT.  Native Type: System.SByte
    ODBC_TinyInt = -6,
    //
    // Summary:
    //     Maps to SQL_BIGINT.  Native Type: System.Int64
    ODBC_BigInt = -5,
    //
    // Summary:
    //     Maps to SQL_LONGVARBINARY.  Native Type: array[System.Byte]
    ODBC_LongVarBinary = -4,
    //
    // Summary:
    //     Maps to SQL_VARBINARY.  Native Type: array[System.Byte]
    ODBC_VarBinary = -3,
    //
    // Summary:
    //     Maps to SQL_BINARY.  Native Type: array[System.Byte]
    ODBC_Binary = -2,
    //
    // Summary:
    //     Maps to SQL_LONGVARCHAR.  Native Type: System.String
    ODBC_LongVarChar = -1,
    //
    // Summary:
    //     Maps to SQL_CHAR.  Native Type: System.String
    ODBC_Char = 1,
    //
    // Summary:
    //     Maps to SQL_NUMERIC.  Native Type: System.Decimal
    ODBC_Numeric = 2,
    //
    // Summary:
    //     Maps to SQL_DECIMAL.  Native Type: System.Decimal
    ODBC_Decimal = 3,
    //
    // Summary:
    //     Maps to SQL_INTEGER.  Native Type: System.Int32
    ODBC_Integer = 4,
    //
    // Summary:
    //     Maps to SQL_SMALLINT.  Native Type: System.Int16
    ODBC_SmallInt = 5,
    //
    // Summary:
    //     Maps to SQL_FLOAT.  Native Type: System.Double
    ODBC_Float = 6,
    //
    // Summary:
    //     Maps to SQL_REAL.  Native Type: System.Single
    ODBC_Real = 7,
    //
    // Summary:
    //     Maps to SQL_DOUBLE.  Native Type: System.Double
    ODBC_Double = 8,
    //
    // Summary:
    //     Maps to SQL_DATETIME. This type should NOT be used with CreateTypeMetadata,
    //     as it is not a valid type identifier.
    ODBC_DateTime = 9,
    //
    // Summary:
    //     Maps to SQL_INTERVAL. Not a valid type identifier.
    ODBC_Interval = 10,
    //
    // Summary:
    //     Maps to SQL_VARCHAR.  Native Type: System.String
    ODBC_VarChar = 12,
    //
    // Summary:
    //     Does not map to an ODBC SQL type.  Native Type: System.DateTimeOffset
    ODBC_DateTimeOffset = 36,
    //
    // Summary:
    //     Maps to SQL_TYPE_DATE.  Native Type: System.DateTime
    ODBC_Type_Date = 91,
    //
    // Summary:
    //     Maps to SQL_TYPE_TIME.  Native Type: System.DateTime
    ODBC_Type_Time = 92,
    //
    // Summary:
    //     Maps to SQL_TYPE_TIMESTAMP.  Native Type: System.DateTime
    ODBC_Type_Timestamp = 93,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_YEAR.  Native Type: Simba.DotNetDSI.DataEngine.DSIMonthSpan
    ODBC_Interval_Year = 101,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_MONTH.  Native Type: Simba.DotNetDSI.DataEngine.DSIMonthSpan
    ODBC_Interval_Month = 102,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_DAY.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Day = 103,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_HOUR.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Hour = 104,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_MINUTE.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Minute = 105,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Second = 106,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_YEAR_TO_MONTH.  Native Type: Simba.DotNetDSI.DataEngine.DSIMonthSpan
    ODBC_Interval_Year_To_Month = 107,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_DAY_TO_HOUR.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Day_To_Hour = 108,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_DAY_TO_MINUTE.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Day_To_Minute = 109,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_DAY_TO_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Day_To_Second = 110,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_HOUR_TO_MINUTE.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Hour_To_Minute = 111,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_HOUR_TO_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Hour_To_Second = 112,
    //
    // Summary:
    //     Maps to SQL_INTERVAL_MINUTE_TO_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
    ODBC_Interval_Minute_To_Second = 113,
};

ODBCTypes JDBC2ODBC ( int jtype );

