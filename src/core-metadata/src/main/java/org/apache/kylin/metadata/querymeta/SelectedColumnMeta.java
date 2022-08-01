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

package org.apache.kylin.metadata.querymeta;

import java.io.Serializable;
import java.sql.Types;

import org.apache.kylin.common.KylinConfig;

/**
 */
@SuppressWarnings("serial")
public class SelectedColumnMeta implements Serializable {
    public SelectedColumnMeta(boolean isAutoIncrement, boolean isCaseSensitive, boolean isSearchable,
            boolean isCurrency, int isNullalbe, boolean isSigned, int displaySize, String label, String name,
            String schemaName, String catelogName, String tableName, int precision, int scale, int columnType,
            String columnTypeName, boolean isReadOnly, boolean isWritable, boolean isDefinitelyWritable) {
        super();
        this.isAutoIncrement = isAutoIncrement;
        this.isCaseSensitive = isCaseSensitive;
        this.isSearchable = isSearchable;
        this.isCurrency = isCurrency;
        this.isNullable = isNullalbe;
        this.isSigned = isSigned;
        this.displaySize = displaySize;
        this.label = label;
        this.name = name;
        this.schemaName = schemaName;
        this.catelogName = catelogName;
        this.tableName = tableName;
        this.precision = precision;
        this.scale = scale;
        this.columnType = columnType;
        this.columnTypeName = columnTypeName;
        this.isReadOnly = isReadOnly;
        this.isWritable = isWritable;
        this.isDefinitelyWritable = isDefinitelyWritable;
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    public boolean isSearchable() {
        return isSearchable;
    }

    public boolean isCurrency() {
        return isCurrency;
    }

    public int getIsNullable() {
        return isNullable;
    }

    public boolean isSigned() {
        return isSigned;
    }

    public int getDisplaySize() {
        if (!KylinConfig.getInstanceFromEnv().isCharDisplaySizeEnabled()) {
            if (columnType == Types.CHAR || columnType == Types.VARCHAR) {
                return -1;
            }
        }
        return displaySize;
    }

    public String getLabel() {
        return label;
    }

    public String getName() {
        return name;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getCatelogName() {
        return catelogName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getColumnType() {
        return columnType;
    }

    public String getColumnTypeName() {
        return columnTypeName;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public boolean isWritable() {
        return isWritable;
    }

    public boolean isDefinitelyWritable() {
        return isDefinitelyWritable;
    }

    private final boolean isAutoIncrement;
    private final boolean isCaseSensitive;
    private final boolean isSearchable;
    private final boolean isCurrency;
    private final int isNullable;// 0:nonull, 1:nullable, 2: nullableunknown
    private final boolean isSigned;
    private final int displaySize;
    private final String label;// AS keyword
    private final String name;
    private final String schemaName;
    private final String catelogName;
    private final String tableName;
    private final int precision;
    private final int scale;
    private final int columnType;// as defined in java.sql.Types
    private final String columnTypeName;
    private final boolean isReadOnly;
    private final boolean isWritable;
    private final boolean isDefinitelyWritable;
}
