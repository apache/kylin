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
import java.util.HashSet;

/**
 * Created by luwei on 17-4-26.
 */
@SuppressWarnings("serial")
public class ColumnMetaWithType extends ColumnMeta {
    public static enum columnTypeEnum implements Serializable {

        DIMENSION, MEASURE, PK, FK

    }

    private HashSet<columnTypeEnum> TYPE;
    private String FULLY_QUALIFIED_COLUMN_NAME;

    public static ColumnMetaWithType ofColumnMeta(ColumnMeta columnMeta) {
        return new ColumnMetaWithType(columnMeta.TABLE_CAT, columnMeta.TABLE_SCHEM, columnMeta.TABLE_NAME,
                columnMeta.COLUMN_NAME, columnMeta.DATA_TYPE, columnMeta.TYPE_NAME, columnMeta.COLUMN_SIZE,
                columnMeta.BUFFER_LENGTH, columnMeta.DECIMAL_DIGITS, columnMeta.NUM_PREC_RADIX, columnMeta.NULLABLE,
                columnMeta.REMARKS, columnMeta.COLUMN_DEF, columnMeta.SQL_DATA_TYPE, columnMeta.SQL_DATETIME_SUB,
                columnMeta.CHAR_OCTET_LENGTH, columnMeta.ORDINAL_POSITION, columnMeta.IS_NULLABLE,
                columnMeta.SCOPE_CATLOG, columnMeta.SCOPE_SCHEMA, columnMeta.SCOPE_TABLE, columnMeta.SOURCE_DATA_TYPE,
                columnMeta.IS_AUTOINCREMENT);
    }

    public ColumnMetaWithType(String tABLE_CAT, String tABLE_SCHEM, String tABLE_NAME, String cOLUMN_NAME,
            int dATA_TYPE, String tYPE_NAME, int cOLUMN_SIZE, int bUFFER_LENGTH, int dECIMAL_DIGITS, int nUM_PREC_RADIX,
            int nULLABLE, String rEMARKS, String cOLUMN_DEF, int sQL_DATA_TYPE, int sQL_DATETIME_SUB,
            int cHAR_OCTET_LENGTH, int oRDINAL_POSITION, String iS_NULLABLE, String sCOPE_CATLOG, String sCOPE_SCHEMA,
            String sCOPE_TABLE, short sOURCE_DATA_TYPE, String iS_AUTOINCREMENT) {
        TABLE_CAT = tABLE_CAT;
        TABLE_SCHEM = tABLE_SCHEM;
        TABLE_NAME = tABLE_NAME;
        COLUMN_NAME = cOLUMN_NAME;
        DATA_TYPE = dATA_TYPE;
        TYPE_NAME = tYPE_NAME;
        COLUMN_SIZE = cOLUMN_SIZE;
        BUFFER_LENGTH = bUFFER_LENGTH;
        DECIMAL_DIGITS = dECIMAL_DIGITS;
        NUM_PREC_RADIX = nUM_PREC_RADIX;
        NULLABLE = nULLABLE;
        REMARKS = rEMARKS;
        COLUMN_DEF = cOLUMN_DEF;
        SQL_DATA_TYPE = sQL_DATA_TYPE;
        SQL_DATETIME_SUB = sQL_DATETIME_SUB;
        CHAR_OCTET_LENGTH = cHAR_OCTET_LENGTH;
        ORDINAL_POSITION = oRDINAL_POSITION;
        IS_NULLABLE = iS_NULLABLE;
        SCOPE_CATLOG = sCOPE_CATLOG;
        SCOPE_SCHEMA = sCOPE_SCHEMA;
        SCOPE_TABLE = sCOPE_TABLE;
        SOURCE_DATA_TYPE = sOURCE_DATA_TYPE;
        IS_AUTOINCREMENT = iS_AUTOINCREMENT;
        TYPE = new HashSet<columnTypeEnum>();
        FULLY_QUALIFIED_COLUMN_NAME = null;
    }


    public String getFULLY_QUALIFIED_COLUMN_NAME() {
        return FULLY_QUALIFIED_COLUMN_NAME;
    }

    public void setFULLY_QUALIFIED_COLUMN_NAME(String fullyQualifiedName) {
        this.FULLY_QUALIFIED_COLUMN_NAME = fullyQualifiedName;
    }

    public HashSet<columnTypeEnum> getTYPE() {
        return TYPE;
    }

    public void setTYPE(HashSet<columnTypeEnum> TYPE) {
        this.TYPE = TYPE;
    }
}
