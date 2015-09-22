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
 
enum JDBCTypes
{
	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>BIT</code>.
	*/
	BIT = -7,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>TINYINT</code>.
	*/
	TINYINT = -6,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>SMALLINT</code>.
	*/
	SMALLINT = 5,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>INTEGER</code>.
	*/
	INTEGER = 4,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>BIGINT</code>.
	*/
	BIGINT = -5,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>FLOAT</code>.
	*/
	FLOAT = 6,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>REAL</code>.
	*/
	REAL = 7,


	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>DOUBLE</code>.
	*/
	DOUBLE = 8,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>NUMERIC</code>.
	*/
	NUMERIC = 2,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>DECIMAL</code>.
	*/
	DECIMAL = 3,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>CHAR</code>.
	*/
	CHAR = 1,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>VARCHAR</code>.
	*/
	VARCHAR = 12,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>LONGVARCHAR</code>.
	*/
	LONGVARCHAR = -1,


	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>DATE</code>.
	*/
	DATE = 91,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>TIME</code>.
	*/
	TIME = 92,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>TIMESTAMP</code>.
	*/
	TIMESTAMP = 93,


	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>BINARY</code>.
	*/
	BINARY = -2,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>VARBINARY</code>.
	*/
	VARBINARY = -3,

	/**
	* <P>The constant in the Java programming language, sometimes referred
	* to as a type code, that identifies the generic SQL type 
	* <code>LONGVARBINARY</code>.
	*/
	LONGVARBINARY = -4,

	/**
	* <P>The constant in the Java programming language
	* that identifies the generic SQL value 
	* <code>NULL</code>.
	*/
	NULL = 0,

	/**
	* The constant in the Java programming language that indicates
	* that the SQL type is database-specific and
	* gets mapped to a Java object that can be accessed via
	* the methods <code>getObject</code> and <code>setObject</code>.
	*/
	OTHER = 1111,



	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type
	* <code>JAVA_OBJECT</code>.
	* @since 1.2
	*/
	JAVA_OBJECT = 2000,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type
	* <code>DISTINCT</code>.
	* @since 1.2
	*/
	DISTINCT = 2001,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type
	* <code>STRUCT</code>.
	* @since 1.2
	*/
	STRUCT = 2002,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type
	* <code>ARRAY</code>.
	* @since 1.2
	*/
	ARRAY = 2003,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type
	* <code>BLOB</code>.
	* @since 1.2
	*/
	BLOB = 2004,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type
	* <code>CLOB</code>.
	* @since 1.2
	*/
	CLOB = 2005,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type
	* <code>REF</code>.
	* @since 1.2
	*/
	REF = 2006,

	/**
	* The constant in the Java programming language, somtimes referred to
	* as a type code, that identifies the generic SQL type <code>DATALINK</code>.
	*
	* @since 1.4
	*/
	DATALINK = 70,

	/**
	* The constant in the Java programming language, somtimes referred to
	* as a type code, that identifies the generic SQL type <code>BOOLEAN</code>.
	*
	* @since 1.4
	*/
	BOOLEAN = 16,

	//------------------------- JDBC 4.0 -----------------------------------

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type <code>ROWID</code>
	* 
	* @since 1.6
	*
	*/
	ROWID = -8,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type <code>NCHAR</code>
	*
	* @since 1.6
	*/
	NCHAR = -15,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type <code>NVARCHAR</code>.
	*
	* @since 1.6
	*/
	NVARCHAR = -9,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type <code>LONGNVARCHAR</code>.
	*
	* @since 1.6
	*/
	LONGNVARCHAR = -16,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type <code>NCLOB</code>.
	*
	* @since 1.6
	*/
	NCLOB = 2011,

	/**
	* The constant in the Java programming language, sometimes referred to
	* as a type code, that identifies the generic SQL type <code>XML</code>.
	*
	* @since 1.6 
	*/
	SQLXML = 2009
};

enum ODBCTypes
{
	// Summary:
	//     Maps to SQL_GUID.
	Guid = -11,
	//
	// Summary:
	//     Maps to SQL_WLONGVARCHAR.  Native Type: System.String
	WLongVarChar = -10,
	//
	// Summary:
	//     Maps to SQL_WVARCHAR.  Native Type: System.String
	WVarChar = -9,
	//
	// Summary:
	//     Maps to SQL_WCHAR.  Native Type: System.String
	WChar = -8,
	//
	// Summary:
	//     Maps to SQL_BIT.  Native Type: System.Boolean
	Bit = -7,
	//
	// Summary:
	//     Maps to SQL_TINYINT.  Native Type: System.SByte
	TinyInt = -6,
	//
	// Summary:
	//     Maps to SQL_BIGINT.  Native Type: System.Int64
	BigInt = -5,
	//
	// Summary:
	//     Maps to SQL_LONGVARBINARY.  Native Type: array[System.Byte]
	LongVarBinary = -4,
	//
	// Summary:
	//     Maps to SQL_VARBINARY.  Native Type: array[System.Byte]
	VarBinary = -3,
	//
	// Summary:
	//     Maps to SQL_BINARY.  Native Type: array[System.Byte]
	Binary = -2,
	//
	// Summary:
	//     Maps to SQL_LONGVARCHAR.  Native Type: System.String
	LongVarChar = -1,
	//
	// Summary:
	//     Maps to SQL_CHAR.  Native Type: System.String
	Char = 1,
	//
	// Summary:
	//     Maps to SQL_NUMERIC.  Native Type: System.Decimal
	Numeric = 2,
	//
	// Summary:
	//     Maps to SQL_DECIMAL.  Native Type: System.Decimal
	Decimal = 3,
	//
	// Summary:
	//     Maps to SQL_INTEGER.  Native Type: System.Int32
	Integer = 4,
	//
	// Summary:
	//     Maps to SQL_SMALLINT.  Native Type: System.Int16
	SmallInt = 5,
	//
	// Summary:
	//     Maps to SQL_FLOAT.  Native Type: System.Double
	Float = 6,
	//
	// Summary:
	//     Maps to SQL_REAL.  Native Type: System.Single
	Real = 7,
	//
	// Summary:
	//     Maps to SQL_DOUBLE.  Native Type: System.Double
	Double = 8,
	//
	// Summary:
	//     Maps to SQL_DATETIME. This type should NOT be used with CreateTypeMetadata,
	//     as it is not a valid type identifier.
	DateTime = 9,
	//
	// Summary:
	//     Maps to SQL_INTERVAL. Not a valid type identifier.
	Interval = 10,
	//
	// Summary:
	//     Maps to SQL_VARCHAR.  Native Type: System.String
	VarChar = 12,
	//
	// Summary:
	//     Does not map to an ODBC SQL type.  Native Type: System.DateTimeOffset
	DateTimeOffset = 36,
	//
	// Summary:
	//     Maps to SQL_TYPE_DATE.  Native Type: System.DateTime
	Type_Date = 91,
	//
	// Summary:
	//     Maps to SQL_TYPE_TIME.  Native Type: System.DateTime
	Type_Time = 92,
	//
	// Summary:
	//     Maps to SQL_TYPE_TIMESTAMP.  Native Type: System.DateTime
	Type_Timestamp = 93,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_YEAR.  Native Type: Simba.DotNetDSI.DataEngine.DSIMonthSpan
	Interval_Year = 101,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_MONTH.  Native Type: Simba.DotNetDSI.DataEngine.DSIMonthSpan
	Interval_Month = 102,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_DAY.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Day = 103,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_HOUR.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Hour = 104,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_MINUTE.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Minute = 105,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Second = 106,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_YEAR_TO_MONTH.  Native Type: Simba.DotNetDSI.DataEngine.DSIMonthSpan
	Interval_Year_To_Month = 107,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_DAY_TO_HOUR.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Day_To_Hour = 108,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_DAY_TO_MINUTE.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Day_To_Minute = 109,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_DAY_TO_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Day_To_Second = 110,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_HOUR_TO_MINUTE.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Hour_To_Minute = 111,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_HOUR_TO_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Hour_To_Second = 112,
	//
	// Summary:
	//     Maps to SQL_INTERVAL_MINUTE_TO_SECOND.  Native Type: Simba.DotNetDSI.DataEngine.DSITimeSpan
	Interval_Minute_To_Second = 113,
};

ODBCTypes JDBC2ODBC(JDBCTypes jtype)
{
	switch (jtype)
	{
	case BIT:
		return ODBCTypes::Bit;
		break;
	case TINYINT:
		return ODBCTypes::TinyInt;
		break;
	case SMALLINT:
		return ODBCTypes::SmallInt;
		break;
	case INTEGER:
		return ODBCTypes::Integer;
		break;
	case BIGINT:
		return ODBCTypes::BigInt;
		break;
	case FLOAT:
		return ODBCTypes::Float;
		break;
	case REAL:
		return ODBCTypes::Real;
		break;
	case DOUBLE:
		return ODBCTypes::Double;
		break;
	case NUMERIC:
		return ODBCTypes::Numeric;
		break;
	case DECIMAL:
		return ODBCTypes::Decimal;
		break;
	case CHAR:
		return ODBCTypes::Char;
		break;
	case VARCHAR:
		return ODBCTypes::VarChar;
		break;
	case LONGVARCHAR:
		return ODBCTypes::LongVarChar;
		break;
	case DATE:
		return ODBCTypes::Type_Date;
		break;
	case TIME:
		return ODBCTypes::Type_Time;
		break;
	case TIMESTAMP:
		return ODBCTypes::Type_Timestamp;
		break;
	case BINARY:
		return ODBCTypes::Binary;
		break;
	case VARBINARY:
		return ODBCTypes::VarBinary;
		break;
	case LONGVARBINARY:
		return ODBCTypes::LongVarBinary;
		break;
	/*case NULL:
		break;
	case OTHER:
		break;
	case JAVA_OBJECT:
		break;
	case DISTINCT:
		break;
	case STRUCT:
		break;
	case ARRAY:
		break;
	case BLOB:
		break;
	case CLOB:
		break;
	case REF:
		break;
	case DATALINK:
		break;
	case BOOLEAN:
		break;
	case ROWID:
		break;
	case NCHAR:
		break;
	case NVARCHAR:
		break;
	case LONGNVARCHAR:
		break;
	case NCLOB:
		break;
	case SQLXML:
		break;*/
	default:
		throw;
		break;
	}
}