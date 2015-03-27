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

package org.apache.kylin.storage.tuple;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.util.DateFormat;

/**
 * @author xjiang
 */
public class Tuple implements ITuple {

    private final TupleInfo info;
    private final Object[] values;

    public Tuple(TupleInfo info) {
        this.info = info;
        this.values = new Object[info.size()];
    }

    public List<String> getAllFields() {
        return info.getAllFields();
    }

    public List<TblColRef> getAllColumns() {
        return info.getAllColumns();
    }

    public Object[] getAllValues() {
        return values;
    }

    public TupleInfo getInfo() {
        return info;
    }

    public String getFieldName(TblColRef col) {
        return info.getFieldName(col);
    }

    public TblColRef getFieldColumn(String fieldName) {
        return info.getColumn(fieldName);
    }

    public Object getValue(String fieldName) {
        int index = info.getFieldIndex(fieldName);
        return values[index];
    }

    public Object getValue(TblColRef col) {
        int index = info.getColumnIndex(col);
        return values[index];
    }

    public String getDataType(String fieldName) {
        return info.getDataType(fieldName);
    }

    private void setFieldObjectValue(String fieldName, Object fieldValue) {
        int index = info.getFieldIndex(fieldName);
        values[index] = fieldValue;
    }

    public void setDimensionValue(String fieldName, String fieldValue) {
        Object objectValue = convertOptiqCellValue(fieldValue, getDataType(fieldName));
        setFieldObjectValue(fieldName, objectValue);
    }

    public void setMeasureValue(String fieldName, Object fieldValue) {
        String dataType = info.getDataType(fieldName);
        // special handling for BigDecimal, allow double be aggregated as
        // BigDecimal during cube build for best precision
        if ("double".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).doubleValue();
        } else if ("integer".equals(dataType) && !(fieldValue instanceof Integer)) {
            fieldValue = ((Number) fieldValue).intValue();
        } else if ("float".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).floatValue();
        }

        setFieldObjectValue(fieldName, fieldValue);
    }

    public boolean hasColumn(TblColRef column) {
        return info.hasColumn(column);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String field : info.getAllFields()) {
            sb.append(field);
            sb.append("=");
            sb.append(getValue(field));
            sb.append(",");
        }
        return sb.toString();
    }

    public static Object convertOptiqCellValue(String strValue, String dataType) {
        if (strValue == null)
            return null;

        if ((strValue.equals("") || strValue.equals("\\N")) && !dataType.equals("string"))
            return null;

        // TODO use data type enum instead of string comparison
        if ("date".equals(dataType)) {
            // convert epoch time
            Date dateValue = DateFormat.stringToDate(strValue); // NOTE: forces GMT timezone
            long millis = dateValue.getTime();
            long days = millis / (1000 * 3600 * 24);
            return Integer.valueOf((int) days); // Optiq expects Integer instead of Long. by honma
        } else if ("tinyint".equals(dataType)) {
            return Byte.valueOf(strValue);
        } else if ("short".equals(dataType) || "smallint".equals(dataType)) {
            return Short.valueOf(strValue);
        } else if ("integer".equals(dataType)) {
            return Integer.valueOf(strValue);
        } else if ("long".equals(dataType) || "bigint".equals(dataType)) {
            return Long.valueOf(strValue);
        } else if ("double".equals(dataType)) {
            return Double.valueOf(strValue);
        } else if ("decimal".equals(dataType)) {
            return new BigDecimal(strValue);
        } else if ("timestamp".equals(dataType)) {
            return Long.valueOf(DateFormat.stringToMillis(strValue));
        } else {
            return strValue;
        }
    }

    // ============================================================================

    public static IDerivedColumnFiller newDerivedColumnFiller(List<TblColRef> rowColumns, TblColRef[] hostCols, DeriveInfo deriveInfo, TupleInfo tupleInfo, CubeManager cubeMgr, CubeSegment cubeSegment) {

        int[] hostIndex = new int[hostCols.length];
        for (int i = 0; i < hostCols.length; i++) {
            hostIndex[i] = rowColumns.indexOf(hostCols[i]);
        }
        String[] derivedFieldNames = new String[deriveInfo.columns.length];
        for (int i = 0; i < deriveInfo.columns.length; i++) {
            derivedFieldNames[i] = tupleInfo.getFieldName(deriveInfo.columns[i]);
        }

        switch (deriveInfo.type) {
        case LOOKUP:
            LookupStringTable lookupTable = cubeMgr.getLookupTable(cubeSegment, deriveInfo.dimension);
            return new LookupFiller(hostIndex, lookupTable, deriveInfo, derivedFieldNames);
        case PK_FK:
            // composite key are split, see CubeDesc.initDimensionColumns()
            return new PKFKFiller(hostIndex[0], derivedFieldNames[0]);
        default:
            throw new IllegalArgumentException();
        }
    }

    public interface IDerivedColumnFiller {
        public void fillDerivedColumns(List<String> rowValues, Tuple tuple);
    }

    static class PKFKFiller implements IDerivedColumnFiller {
        final int hostIndex;
        final String derivedFieldName;

        public PKFKFiller(int hostIndex, String derivedFieldName) {
            this.hostIndex = hostIndex;
            this.derivedFieldName = derivedFieldName;
        }

        @Override
        public void fillDerivedColumns(List<String> rowValues, Tuple tuple) {
            String value = rowValues.get(hostIndex);
            tuple.setDimensionValue(derivedFieldName, value);
        }
    }

    static class LookupFiller implements IDerivedColumnFiller {

        final int[] hostIndex;
        final int hostLen;
        final Array<String> lookupKey;
        final LookupStringTable lookupTable;
        final int[] derivedIndex;
        final int derivedLen;
        final String[] derivedFieldNames;

        public LookupFiller(int[] hostIndex, LookupStringTable lookupTable, DeriveInfo deriveInfo, String[] derivedFieldNames) {
            this.hostIndex = hostIndex;
            this.hostLen = hostIndex.length;
            this.lookupKey = new Array<String>(new String[hostLen]);
            this.lookupTable = lookupTable;
            this.derivedIndex = new int[deriveInfo.columns.length];
            this.derivedLen = derivedIndex.length;
            this.derivedFieldNames = derivedFieldNames;

            for (int i = 0; i < derivedLen; i++) {
                derivedIndex[i] = deriveInfo.columns[i].getColumn().getZeroBasedIndex();
            }
        }

        @Override
        public void fillDerivedColumns(List<String> rowValues, Tuple tuple) {
            for (int i = 0; i < hostLen; i++) {
                lookupKey.data[i] = rowValues.get(hostIndex[i]);
            }

            String[] lookupRow = lookupTable.getRow(lookupKey);

            if (lookupRow != null) {
                for (int i = 0; i < derivedLen; i++) {
                    String value = lookupRow[derivedIndex[i]];
                    tuple.setDimensionValue(derivedFieldNames[i], value);
                }
            } else {
                for (int i = 0; i < derivedLen; i++) {
                    tuple.setDimensionValue(derivedFieldNames[i], null);
                }
            }
        }
    }
}
