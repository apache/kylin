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


#include "JDBCODBC.h"

ODBCTypes JDBC2ODBC ( int jtype )
{
    JDBCTypes temp = ( JDBCTypes ) jtype;

    switch ( temp )
    {
        case JDBC_BOOLEAN :
            return ODBCTypes::ODBC_Bit ;
            break;

        case JDBC_BIT :
            return ODBCTypes::ODBC_Bit;
            break;

        case JDBC_TINYINT :
            return ODBCTypes::ODBC_TinyInt;
            break;

        case JDBC_SMALLINT :
            return ODBCTypes::ODBC_SmallInt;
            break;

        case JDBC_INTEGER :
            return ODBCTypes::ODBC_Integer;
            break;

        case JDBC_BIGINT :
            return ODBCTypes::ODBC_BigInt;
            break;

        case JDBC_FLOAT :
            return ODBCTypes::ODBC_Float;
            break;

        case JDBC_REAL :
            return ODBCTypes::ODBC_Real;
            break;

        case JDBC_DOUBLE :
            return ODBCTypes::ODBC_Double;
            break;

        case JDBC_NUMERIC :
            return ODBCTypes::ODBC_Numeric;
            break;

        case JDBC_DECIMAL :
            return ODBCTypes::ODBC_Decimal;
            break;

        case JDBC_CHAR :
            return ODBCTypes::ODBC_Char;
            //return ODBCTypes::ODBC_WChar;//it's a unicode dirver
            break;

        case JDBC_VARCHAR :
            return ODBCTypes::ODBC_VarChar;
            //return ODBCTypes::ODBC_WChar;//it's a unicode dirver
            break;

        case 2000 : //"ANY" type in KYLIN
            return ODBCTypes::ODBC_WChar;//it's a unicode dirver
            break;

        case JDBC_LONGVARCHAR :
            return ODBCTypes::ODBC_LongVarChar;
            break;

        case JDBC_DATE :
            return ODBCTypes::ODBC_Type_Date;
            break;

        case JDBC_TIME :
            return ODBCTypes::ODBC_Type_Time;
            break;

        case JDBC_TIMESTAMP :
            return ODBCTypes::ODBC_Type_Timestamp;
            break;

        case JDBC_BINARY :
            return ODBCTypes::ODBC_Binary;
            break;

        case JDBC_VARBINARY :
            return ODBCTypes::ODBC_VarBinary;
            break;

        case JDBC_LONGVARBINARY :
            return ODBCTypes::ODBC_LongVarBinary;
            break;

            /*  case NULL:
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
        default :
            throw;
            break;
    }
};

