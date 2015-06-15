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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class TableMetaStub {

    private static final long serialVersionUID = 1L;
    private String TABLE_CAT;
    private String TABLE_SCHEM;
    private String TABLE_NAME;
    private String TABLE_TYPE;
    private String REMARKS;
    private String TYPE_CAT;
    private String TYPE_SCHEM;
    private String TYPE_NAME;
    private String SELF_REFERENCING_COL_NAME;
    private String REF_GENERATION;
    @JsonProperty("columns")
    private List<ColumnMetaStub> columns = new ArrayList<ColumnMetaStub>();

    public String getTABLE_CAT() {
        return TABLE_CAT;
    }

    public void setTABLE_CAT(String tABLE_CAT) {
        TABLE_CAT = tABLE_CAT;
    }

    public String getTABLE_SCHEM() {
        return TABLE_SCHEM;
    }

    public void setTABLE_SCHEM(String tABLE_SCHEM) {
        TABLE_SCHEM = tABLE_SCHEM;
    }

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String tABLE_NAME) {
        TABLE_NAME = tABLE_NAME;
    }

    public String getTABLE_TYPE() {
        return TABLE_TYPE;
    }

    public void setTABLE_TYPE(String tABLE_TYPE) {
        TABLE_TYPE = tABLE_TYPE;
    }

    public String getREMARKS() {
        return REMARKS;
    }

    public void setREMARKS(String rEMARKS) {
        REMARKS = rEMARKS;
    }

    public String getTYPE_CAT() {
        return TYPE_CAT;
    }

    public void setTYPE_CAT(String tYPE_CAT) {
        TYPE_CAT = tYPE_CAT;
    }

    public String getTYPE_SCHEM() {
        return TYPE_SCHEM;
    }

    public void setTYPE_SCHEM(String tYPE_SCHEM) {
        TYPE_SCHEM = tYPE_SCHEM;
    }

    public String getTYPE_NAME() {
        return TYPE_NAME;
    }

    public void setTYPE_NAME(String tYPE_NAME) {
        TYPE_NAME = tYPE_NAME;
    }

    public String getSELF_REFERENCING_COL_NAME() {
        return SELF_REFERENCING_COL_NAME;
    }

    public void setSELF_REFERENCING_COL_NAME(String sELF_REFERENCING_COL_NAME) {
        SELF_REFERENCING_COL_NAME = sELF_REFERENCING_COL_NAME;
    }

    public String getREF_GENERATION() {
        return REF_GENERATION;
    }

    public void setREF_GENERATION(String rEF_GENERATION) {
        REF_GENERATION = rEF_GENERATION;
    }

    public List<ColumnMetaStub> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMetaStub> columns) {
        this.columns = columns;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public static class ColumnMetaStub {
        private String TABLE_CAT;
        private String TABLE_SCHEM;
        private String TABLE_NAME;
        private String COLUMN_NAME;
        private int DATA_TYPE;
        private String TYPE_NAME;
        private int COLUMN_SIZE;
        private int BUFFER_LENGTH;
        private int DECIMAL_DIGITS;
        private int NUM_PREC_RADIX;
        private int NULLABLE;
        private String REMARKS;
        private String COLUMN_DEF;
        private int SQL_DATA_TYPE;
        private int SQL_DATETIME_SUB;
        private int CHAR_OCTET_LENGTH;
        private int ORDINAL_POSITION;
        private String IS_NULLABLE;
        private String SCOPE_CATLOG;
        private String SCOPE_SCHEMA;
        private String SCOPE_TABLE;
        private short SOURCE_DATA_TYPE;
        private String IS_AUTOINCREMENT;

        public String getTABLE_CAT() {
            return TABLE_CAT;
        }

        public void setTABLE_CAT(String tABLE_CAT) {
            TABLE_CAT = tABLE_CAT;
        }

        public String getTABLE_SCHEM() {
            return TABLE_SCHEM;
        }

        public void setTABLE_SCHEM(String tABLE_SCHEM) {
            TABLE_SCHEM = tABLE_SCHEM;
        }

        public String getTABLE_NAME() {
            return TABLE_NAME;
        }

        public void setTABLE_NAME(String tABLE_NAME) {
            TABLE_NAME = tABLE_NAME;
        }

        public String getCOLUMN_NAME() {
            return COLUMN_NAME;
        }

        public void setCOLUMN_NAME(String cOLUMN_NAME) {
            COLUMN_NAME = cOLUMN_NAME;
        }

        public int getDATA_TYPE() {
            return DATA_TYPE;
        }

        public void setDATA_TYPE(int dATA_TYPE) {
            DATA_TYPE = dATA_TYPE;
        }

        public String getTYPE_NAME() {
            return TYPE_NAME;
        }

        public void setTYPE_NAME(String tYPE_NAME) {
            TYPE_NAME = tYPE_NAME;
        }

        public int getCOLUMN_SIZE() {
            return COLUMN_SIZE;
        }

        public void setCOLUMN_SIZE(int cOLUMN_SIZE) {
            COLUMN_SIZE = cOLUMN_SIZE;
        }

        public int getBUFFER_LENGTH() {
            return BUFFER_LENGTH;
        }

        public void setBUFFER_LENGTH(int bUFFER_LENGTH) {
            BUFFER_LENGTH = bUFFER_LENGTH;
        }

        public int getDECIMAL_DIGITS() {
            return DECIMAL_DIGITS;
        }

        public void setDECIMAL_DIGITS(int dECIMAL_DIGITS) {
            DECIMAL_DIGITS = dECIMAL_DIGITS;
        }

        public int getNUM_PREC_RADIX() {
            return NUM_PREC_RADIX;
        }

        public void setNUM_PREC_RADIX(int nUM_PREC_RADIX) {
            NUM_PREC_RADIX = nUM_PREC_RADIX;
        }

        public int getNULLABLE() {
            return NULLABLE;
        }

        public void setNULLABLE(int nULLABLE) {
            NULLABLE = nULLABLE;
        }

        public String getREMARKS() {
            return REMARKS;
        }

        public void setREMARKS(String rEMARKS) {
            REMARKS = rEMARKS;
        }

        public String getCOLUMN_DEF() {
            return COLUMN_DEF;
        }

        public void setCOLUMN_DEF(String cOLUMN_DEF) {
            COLUMN_DEF = cOLUMN_DEF;
        }

        public int getSQL_DATA_TYPE() {
            return SQL_DATA_TYPE;
        }

        public void setSQL_DATA_TYPE(int sQL_DATA_TYPE) {
            SQL_DATA_TYPE = sQL_DATA_TYPE;
        }

        public int getSQL_DATETIME_SUB() {
            return SQL_DATETIME_SUB;
        }

        public void setSQL_DATETIME_SUB(int sQL_DATETIME_SUB) {
            SQL_DATETIME_SUB = sQL_DATETIME_SUB;
        }

        public int getCHAR_OCTET_LENGTH() {
            return CHAR_OCTET_LENGTH;
        }

        public void setCHAR_OCTET_LENGTH(int cHAR_OCTET_LENGTH) {
            CHAR_OCTET_LENGTH = cHAR_OCTET_LENGTH;
        }

        public int getORDINAL_POSITION() {
            return ORDINAL_POSITION;
        }

        public void setORDINAL_POSITION(int oRDINAL_POSITION) {
            ORDINAL_POSITION = oRDINAL_POSITION;
        }

        public String getIS_NULLABLE() {
            return IS_NULLABLE;
        }

        public void setIS_NULLABLE(String iS_NULLABLE) {
            IS_NULLABLE = iS_NULLABLE;
        }

        public String getSCOPE_CATLOG() {
            return SCOPE_CATLOG;
        }

        public void setSCOPE_CATLOG(String sCOPE_CATLOG) {
            SCOPE_CATLOG = sCOPE_CATLOG;
        }

        public String getSCOPE_SCHEMA() {
            return SCOPE_SCHEMA;
        }

        public void setSCOPE_SCHEMA(String sCOPE_SCHEMA) {
            SCOPE_SCHEMA = sCOPE_SCHEMA;
        }

        public String getSCOPE_TABLE() {
            return SCOPE_TABLE;
        }

        public void setSCOPE_TABLE(String sCOPE_TABLE) {
            SCOPE_TABLE = sCOPE_TABLE;
        }

        public short getSOURCE_DATA_TYPE() {
            return SOURCE_DATA_TYPE;
        }

        public void setSOURCE_DATA_TYPE(short sOURCE_DATA_TYPE) {
            SOURCE_DATA_TYPE = sOURCE_DATA_TYPE;
        }

        public String getIS_AUTOINCREMENT() {
            return IS_AUTOINCREMENT;
        }

        public void setIS_AUTOINCREMENT(String iS_AUTOINCREMENT) {
            this.IS_AUTOINCREMENT = iS_AUTOINCREMENT;
        }
    }
}
