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


// ----------------------------------------------------------------------------
//
// File:        MsgTypes.h
//
// Purpose:     Message protocols for kylin deriver to connect with Rest Server
//
// Author:      Hongbin Ma
//

#pragma once

#include <string>
#include <vector>
#include <sstream>

#include "StringUtils.h"
#include "JDBCODBC.h"

#define ASSIGN_IF_NOT_NULL(x,y,z)  if(!y.is_null())x=y.z
#define x_ASSIGN_IF_NOT_NULL(x,y,z)  if(!y.is_null())x=wstring2string(y.z)

using namespace std;

class TableMeta
{
    public:
    string TABLE_CAT;
    string TABLE_SCHEM;
    string TABLE_NAME;
    string TABLE_TYPE;
    string REMARKS;
    string TYPE_CAT;
    string TYPE_SCHEM;
    string TYPE_NAME;
    string SELF_REFERENCING_COL_NAME;
    string REF_GENERATION;
};

class ColumnMeta
{
    public:
    string TABLE_CAT;
    string TABLE_SCHEM;
    string TABLE_NAME;
    string COLUMN_NAME;
    int DATA_TYPE;
    string TYPE_NAME;
    int COLUMN_SIZE;
    int BUFFER_LENGTH;
    int DECIMAL_DIGITS;
    int NUM_PREC_RADIX;
    int NULLABLE;
    string REMARKS;
    string COLUMN_DEF;
    int SQL_DATA_TYPE;
    int SQL_DATETIME_SUB;
    int CHAR_OCTET_LENGTH;
    int ORDINAL_POSITION;
    string IS_NULLABLE;
    string SCOPE_CATLOG;
    string SCOPE_SCHEMA;
    string SCOPE_TABLE;
    short SOURCE_DATA_TYPE;
    string IS_AUTOINCREMENT;
};

class MetadataResponse
{
    public:
    std::vector <TableMeta*> tableMetas;
    std::vector <ColumnMeta*> columnMetas;

    ~MetadataResponse ()
    {
        for ( std::vector <TableMeta*>::size_type i = 0; i < tableMetas . size (); ++i )
        {
            TableMeta* p = tableMetas . at ( i );
            delete p;
        }

        for ( std::vector <ColumnMeta*>::size_type i = 0; i < columnMetas . size (); ++i )
        {
            ColumnMeta* p = columnMetas . at ( i );
            delete p;
        }
    }
};

class SelectedColumnMeta
{
    public:
    bool isAutoIncrement;
    bool isCaseSensitive;
    bool isSearchable;
    bool isCurrency;
    int isNullable;//0:nonull, 1:nullable, 2: nullableunknown
    bool isSigned;
    int displaySize;
    string label;// AS keyword
    string name;
    string schemaName;
    string catelogName;
    string tableName;
    int precision;
    int scale;
    int columnType;// the orig value passed from REST is java.sql.Types, we convert it to SQL Type
    string columnTypeName;
    bool isReadOnly;
    bool isWritable;
    bool isDefinitelyWritable;
};

class SQLRowContent
{
    public:
    std::vector <wstring> contents;
};

class SQLResponse
{
    public:
    // the data type for each column
    std::vector <SelectedColumnMeta*> columnMetas;

    // the results rows, each row contains several columns
    std::vector <SQLRowContent*> results;

    // if not select query, only return affected row count
    int affectedRowCount;

    // flag indicating whether an exception occurred
    bool isException;

    // if isException, the detailed exception message
    wstring exceptionMessage;

    ~SQLResponse ()
    {
        for ( std::vector <SelectedColumnMeta*>::size_type i = 0; i < columnMetas . size (); ++i )
        {
            SelectedColumnMeta* p = columnMetas . at ( i );
            delete p;
        }

        for ( std::vector <SQLRowContent*>::size_type i = 0; i < results . size (); ++i )
        {
            SQLRowContent* p = results . at ( i );
            delete p;
        }
    }

    static std::unique_ptr <SQLResponse> MakeResp4SQLTables ( MetadataResponse* meta )
    {
        std::unique_ptr <SQLResponse> ret ( new SQLResponse () );
        FillColumnMetas4SQLTables ( ret . get () );

        for ( auto i = meta -> tableMetas . begin (); i != meta -> tableMetas . end (); i++ )
        {
            SQLRowContent* temp = new SQLRowContent ();
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TABLE_CAT ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TABLE_SCHEM ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TABLE_NAME ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TABLE_TYPE ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> REMARKS ) );
            ret -> results . push_back ( temp );
        }

        return ret;
    }

    static std::unique_ptr <SQLResponse> MakeResp4SQLColumns ( MetadataResponse* meta, char* tableName, char* columnName )
    {
        std::unique_ptr <SQLResponse> ret ( new SQLResponse () );
        FillColumnMetas4SQLColumns ( ret . get () );

        for ( auto i = meta -> columnMetas . begin (); i != meta -> columnMetas . end (); i++ )
        {
            //filter
            if ( tableName != NULL && _stricmp ( tableName, ( *i ) -> TABLE_NAME . c_str () ) != 0 )
            {
                continue;
            }

            if ( columnName != NULL && _stricmp ( columnName, ( *i ) -> COLUMN_NAME . c_str () ) != 0 )
            {
                continue;
            }

            SQLRowContent* temp = new SQLRowContent ();
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TABLE_CAT ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TABLE_SCHEM ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TABLE_NAME ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> COLUMN_NAME ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> DATA_TYPE ) ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> TYPE_NAME ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> COLUMN_SIZE ) ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> BUFFER_LENGTH ) ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> DECIMAL_DIGITS ) ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> NUM_PREC_RADIX ) ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> NULLABLE ) ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> REMARKS ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> COLUMN_DEF ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> SQL_DATA_TYPE ) ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> SQL_DATETIME_SUB ) ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> CHAR_OCTET_LENGTH ) ) );
            temp -> contents . push_back ( string2wstring ( std::to_string ( ( *i ) -> ORDINAL_POSITION ) ) );
            temp -> contents . push_back ( string2wstring ( ( *i ) -> IS_NULLABLE ) );
            temp -> contents . push_back ( L"0" ); //user_data_type
            ret -> results . push_back ( temp );
        }

        return ret;
    }

    static std::string GetString ( int i )
    {
        std::ostringstream ss;
        ss << i;
        return ss . str ();
    }

    static void FillColumnMetas4SQLTables ( SQLResponse* sqlResp )
    {
        SelectedColumnMeta* m1 = new SelectedColumnMeta ();
        m1 -> label = "TABLE_CAT";
        m1 -> name = "TABLE_CAT";
        m1 -> displaySize = 128;
        m1 -> scale = 0;
        m1 -> isNullable = 1;
        m1 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m2 = new SelectedColumnMeta ();
        m2 -> label = "TABLE_SCHEM";
        m2 -> name = "TABLE_SCHEM";
        m2 -> displaySize = 128;
        m2 -> scale = 0;
        m2 -> isNullable = 1;
        m2 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m3 = new SelectedColumnMeta ();
        m3 -> label = "TABLE_NAME";
        m3 -> name = "TABLE_NAME";
        m3 -> displaySize = 128;
        m3 -> scale = 0;
        m3 -> isNullable = 1;
        m3 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m4 = new SelectedColumnMeta ();
        m4 -> label = "TABLE_TYPE";
        m4 -> name = "TABLE_TYPE";
        m4 -> displaySize = 32;
        m4 -> scale = 0;
        m4 -> isNullable = 1;
        m4 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m5 = new SelectedColumnMeta ();
        m5 -> label = "REMARKS";
        m5 -> name = "REMARKS";
        m5 -> displaySize = 254;
        m5 -> scale = 0;
        m5 -> isNullable = 1;
        m5 -> columnType = ODBCTypes::ODBC_WVarChar;
        sqlResp -> columnMetas . push_back ( m1 );
        sqlResp -> columnMetas . push_back ( m2 );
        sqlResp -> columnMetas . push_back ( m3 );
        sqlResp -> columnMetas . push_back ( m4 );
        sqlResp -> columnMetas . push_back ( m5 );
    }

    static void FillColumnMetas4SQLColumns ( SQLResponse* sqlResp )
    {
        SelectedColumnMeta* m1 = new SelectedColumnMeta ();
        m1 -> label = "TABLE_CAT";
        m1 -> name = "TABLE_CAT";
        m1 -> displaySize = 128;
        m1 -> scale = 0;
        m1 -> isNullable = 1;
        m1 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m2 = new SelectedColumnMeta ();
        m2 -> label = "TABLE_SCHEM";
        m2 -> name = "TABLE_SCHEM";
        m2 -> displaySize = 128;
        m2 -> scale = 0;
        m2 -> isNullable = 1;
        m2 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m3 = new SelectedColumnMeta ();
        m3 -> label = "TABLE_NAME";
        m3 -> name = "TABLE_NAME";
        m3 -> displaySize = 128;
        m3 -> scale = 0;
        m3 -> isNullable = 1;
        m3 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m4 = new SelectedColumnMeta ();
        m4 -> label = "COLUMN_NAME";
        m4 -> name = "COLUMN_NAME";
        m4 -> displaySize = 128;
        m4 -> scale = 0;
        m4 -> isNullable = 0;
        m4 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m5 = new SelectedColumnMeta ();
        m5 -> label = "DATA_TYPE";
        m5 -> name = "DATA_TYPE";
        m5 -> displaySize = 5;
        m5 -> scale = 0;
        m5 -> isNullable = 0;
        m5 -> columnType = ODBCTypes::ODBC_SmallInt;
        SelectedColumnMeta* m6 = new SelectedColumnMeta ();
        m6 -> label = "TYPE_NAME";
        m6 -> name = "TYPE_NAME";
        m6 -> displaySize = 128;
        m6 -> scale = 0;
        m6 -> isNullable = 0;
        m6 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m7 = new SelectedColumnMeta ();
        m7 -> label = "COLUMN_SIZE";
        m7 -> name = "COLUMN_SIZE";
        m7 -> displaySize = 10;
        m7 -> scale = 0;
        m7 -> isNullable = 1;
        m7 -> columnType = ODBCTypes::ODBC_Integer;
        SelectedColumnMeta* m8 = new SelectedColumnMeta ();
        m8 -> label = "BUFFER_LENGTH";
        m8 -> name = "BUFFER_LENGTH";
        m8 -> displaySize = 10;
        m8 -> scale = 0;
        m8 -> isNullable = 1;
        m8 -> columnType = ODBCTypes::ODBC_Integer;
        SelectedColumnMeta* m9 = new SelectedColumnMeta ();
        m9 -> label = "DECIMAL_DIGITS";
        m9 -> name = "DECIMAL_DIGITS";
        m9 -> displaySize = 5;
        m9 -> scale = 0;
        m9 -> isNullable = 1;
        m9 -> columnType = ODBCTypes::ODBC_SmallInt;
        SelectedColumnMeta* m10 = new SelectedColumnMeta ();
        m10 -> label = "NUM_PREC_RADIX";
        m10 -> name = "NUM_PREC_RADIX";
        m10 -> displaySize = 5;
        m10 -> scale = 0;
        m10 -> isNullable = 1;
        m10 -> columnType = ODBCTypes::ODBC_SmallInt;
        SelectedColumnMeta* m11 = new SelectedColumnMeta ();
        m11 -> label = "NULLABLE";
        m11 -> name = "NULLABLE";
        m11 -> displaySize = 5;
        m11 -> scale = 0;
        m11 -> isNullable = 0;
        m11 -> columnType = ODBCTypes::ODBC_SmallInt;
        SelectedColumnMeta* m12 = new SelectedColumnMeta ();
        m12 -> label = "REMARKS";
        m12 -> name = "REMARKS";
        m12 -> displaySize = 128;
        m12 -> scale = 0;
        m12 -> isNullable = 1;
        m12 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m13 = new SelectedColumnMeta ();
        m13 -> label = "COLUMN_DEF";
        m13 -> name = "COLUMN_DEF";
        m13 -> displaySize = 4000;
        m13 -> scale = 0;
        m13 -> isNullable = 1;
        m13 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m14 = new SelectedColumnMeta ();
        m14 -> label = "SQL_DATA_TYPE";
        m14 -> name = "SQL_DATA_TYPE";
        m14 -> displaySize = 5;
        m14 -> scale = 0;
        m14 -> isNullable = 0;
        m14 -> columnType = ODBCTypes::ODBC_SmallInt;
        SelectedColumnMeta* m15 = new SelectedColumnMeta ();
        m15 -> label = "SQL_DATETIME_SUB";
        m15 -> name = "SQL_DATETIME_SUB";
        m15 -> displaySize = 5;
        m15 -> scale = 0;
        m15 -> isNullable = 1;
        m15 -> columnType = ODBCTypes::ODBC_SmallInt;
        SelectedColumnMeta* m16 = new SelectedColumnMeta ();
        m16 -> label = "CHAR_OCTET_LENGTH";
        m16 -> name = "CHAR_OCTET_LENGTH";
        m16 -> displaySize = 10;
        m16 -> scale = 0;
        m16 -> isNullable = 1;
        m16 -> columnType = ODBCTypes::ODBC_Integer;
        SelectedColumnMeta* m17 = new SelectedColumnMeta ();
        m17 -> label = "ORDINAL_POSITION";
        m17 -> name = "ORDINAL_POSITION";
        m17 -> displaySize = 10;
        m17 -> scale = 0;
        m17 -> isNullable = 0;
        m17 -> columnType = ODBCTypes::ODBC_Integer;
        SelectedColumnMeta* m18 = new SelectedColumnMeta ();
        m18 -> label = "IS_NULLABLE";
        m18 -> name = "IS_NULLABLE";
        m18 -> displaySize = 254;
        m18 -> scale = 0;
        m18 -> isNullable = 1;
        m18 -> columnType = ODBCTypes::ODBC_WVarChar;
        SelectedColumnMeta* m19 = new SelectedColumnMeta ();
        m19 -> label = "USER_DATA_TYPE";
        m19 -> name = "USER_DATA_TYPE";
        m19 -> displaySize = 5;
        m19 -> scale = 0;
        m19 -> isNullable = 1;
        m19 -> columnType = ODBCTypes::ODBC_SmallInt;
        sqlResp -> columnMetas . push_back ( m1 );
        sqlResp -> columnMetas . push_back ( m2 );
        sqlResp -> columnMetas . push_back ( m3 );
        sqlResp -> columnMetas . push_back ( m4 );
        sqlResp -> columnMetas . push_back ( m5 );
        sqlResp -> columnMetas . push_back ( m6 );
        sqlResp -> columnMetas . push_back ( m7 );
        sqlResp -> columnMetas . push_back ( m8 );
        sqlResp -> columnMetas . push_back ( m9 );
        sqlResp -> columnMetas . push_back ( m10 );
        sqlResp -> columnMetas . push_back ( m11 );
        sqlResp -> columnMetas . push_back ( m12 );
        sqlResp -> columnMetas . push_back ( m13 );
        sqlResp -> columnMetas . push_back ( m14 );
        sqlResp -> columnMetas . push_back ( m15 );
        sqlResp -> columnMetas . push_back ( m16 );
        sqlResp -> columnMetas . push_back ( m17 );
        sqlResp -> columnMetas . push_back ( m18 );
        sqlResp -> columnMetas . push_back ( m19 );
    }
};


class ErrorMessage
{
    public:
    wstring url;

    // if isException, the detailed exception message
    wstring msg;
};

