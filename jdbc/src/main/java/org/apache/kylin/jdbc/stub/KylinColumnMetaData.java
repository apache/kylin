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

package org.apache.kylin.jdbc.stub;

import java.sql.DatabaseMetaData;

import net.hydromatic.avatica.ColumnMetaData;

/**
 * @author xduo
 * 
 */
public class KylinColumnMetaData extends ColumnMetaData {

    public KylinColumnMetaData(int ordinal, boolean autoIncrement, boolean caseSensitive, boolean searchable, boolean currency, int nullable, boolean signed, int displaySize, String label, String columnName, String schemaName, int precision, int scale, String tableName, String catalogName, AvaticaType type, boolean readOnly, boolean writable, boolean definitelyWritable, String columnClassName) {
        super(ordinal, autoIncrement, caseSensitive, searchable, currency, nullable, signed, displaySize, label, columnName, schemaName, precision, scale, tableName, catalogName, type, readOnly, writable, definitelyWritable, columnClassName);
    }

    public static ColumnMetaData dummy(int ordinal, String label, String columnName, AvaticaType type, boolean nullable) {
        return new ColumnMetaData(ordinal, false, true, false, false, nullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls, true, -1, label, columnName, null, -1, -1, null, null, type, true, false, false, null);
    }
}
