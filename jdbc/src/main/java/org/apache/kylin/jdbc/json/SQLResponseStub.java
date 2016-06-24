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

package org.apache.kylin.jdbc.json;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SQLResponseStub implements Serializable {
    private static final long serialVersionUID = 1L;

    // private static final Logger logger =
    // LoggerFactory.getLogger(SQLResponse.class);

    // the data type for each column
    private List<ColumnMetaStub> columnMetas;

    // the results rows, each row contains several columns
    private List<String[]> results;

    private String cube;

    // if not select query, only return affected row count
    private int affectedRowCount;

    // if isException, the detailed exception message
    private String exceptionMessage;

    private boolean isException;

    private long duration;

    private boolean isPartial = false;

    private long totalScanCount;

    private boolean hitExceptionCache = false;

    private boolean storageCacheUsed = false;

    public SQLResponseStub() {
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public List<ColumnMetaStub> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMetaStub> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public List<String[]> getResults() {
        return results;
    }

    public void setResults(List<String[]> results) {
        this.results = results;
    }

    public String getCube() {
        return cube;
    }

    public void setCube(String cube) {
        this.cube = cube;
    }

    public int getAffectedRowCount() {
        return affectedRowCount;
    }

    public void setAffectedRowCount(int affectedRowCount) {
        this.affectedRowCount = affectedRowCount;
    }

    public boolean getIsException() {
        return isException;
    }

    public void setIsException(boolean isException) {
        this.isException = isException;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public boolean isPartial() {
        return isPartial;
    }

    public void setPartial(boolean isPartial) {
        this.isPartial = isPartial;
    }

    public long getTotalScanCount() {
        return totalScanCount;
    }

    public void setTotalScanCount(long totalScanCount) {
        this.totalScanCount = totalScanCount;
    }

    public boolean isHitExceptionCache() {
        return hitExceptionCache;
    }

    public void setHitExceptionCache(boolean hitExceptionCache) {
        this.hitExceptionCache = hitExceptionCache;
    }

    public boolean isStorageCacheUsed() {
        return storageCacheUsed;
    }

    public void setStorageCacheUsed(boolean storageCacheUsed) {
        this.storageCacheUsed = storageCacheUsed;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ColumnMetaStub {

        private boolean isAutoIncrement;
        private boolean isCaseSensitive;
        private boolean isSearchable;
        private boolean isCurrency;
        private int isNullable;// 0:nonull, 1:nullable, 2: nullableunknown
        private boolean isSigned;
        private int displaySize;
        private String label;// AS keyword
        private String name;
        private String schemaName;
        private String catelogName;
        private String tableName;
        private int precision;
        private int scale;
        private int columnType;// as defined in java.sql.Types
        private String columnTypeName;
        private boolean isReadOnly;
        private boolean isWritable;
        private boolean isDefinitelyWritable;

        public ColumnMetaStub() {
        }

        public boolean isAutoIncrement() {
            return isAutoIncrement;
        }

        public void setAutoIncrement(boolean isAutoIncrement) {
            this.isAutoIncrement = isAutoIncrement;
        }

        public boolean isCaseSensitive() {
            return isCaseSensitive;
        }

        public void setCaseSensitive(boolean isCaseSensitive) {
            this.isCaseSensitive = isCaseSensitive;
        }

        public boolean isSearchable() {
            return isSearchable;
        }

        public void setSearchable(boolean isSearchable) {
            this.isSearchable = isSearchable;
        }

        public boolean isCurrency() {
            return isCurrency;
        }

        public void setCurrency(boolean isCurrency) {
            this.isCurrency = isCurrency;
        }

        public int getIsNullable() {
            return isNullable;
        }

        public void setIsNullable(int isNullable) {
            this.isNullable = isNullable;
        }

        public boolean isSigned() {
            return isSigned;
        }

        public void setSigned(boolean isSigned) {
            this.isSigned = isSigned;
        }

        public int getDisplaySize() {
            return displaySize;
        }

        public void setDisplaySize(int displaySize) {
            this.displaySize = displaySize;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getCatelogName() {
            return catelogName;
        }

        public void setCatelogName(String catelogName) {
            this.catelogName = catelogName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public int getPrecision() {
            return precision;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public int getScale() {
            return scale;
        }

        public void setScale(int scale) {
            this.scale = scale;
        }

        public int getColumnType() {
            return columnType;
        }

        public void setColumnType(int columnType) {
            this.columnType = columnType;
        }

        public String getColumnTypeName() {
            return columnTypeName;
        }

        public void setColumnTypeName(String columnTypeName) {
            this.columnTypeName = columnTypeName;
        }

        public boolean isReadOnly() {
            return isReadOnly;
        }

        public void setReadOnly(boolean isReadOnly) {
            this.isReadOnly = isReadOnly;
        }

        public boolean isWritable() {
            return isWritable;
        }

        public void setWritable(boolean isWritable) {
            this.isWritable = isWritable;
        }

        public boolean isDefinitelyWritable() {
            return isDefinitelyWritable;
        }

        public void setDefinitelyWritable(boolean isDefinitelyWritable) {
            this.isDefinitelyWritable = isDefinitelyWritable;
        }

    }
}
