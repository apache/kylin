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

package org.apache.kylin.metadata.tuple;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.TblColRef;

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

    public void setFieldObjectValue(int index, Object fieldValue) {
        values[index] = fieldValue;
    }

    public void setFieldObjectValue(String fieldName, Object fieldValue) {
        int index = info.getFieldIndex(fieldName);
        values[index] = fieldValue;
    }

    public void setDimensionValue(String fieldName, String fieldValue) {
        Object objectValue = convertOptiqCellValue(fieldValue, getDataType(fieldName));
        setFieldObjectValue(fieldName, objectValue);
    }

    public void setMeasureValue(int idx, Object fieldValue) {
        String dataType = info.getDataType(idx);
        // special handling for BigDecimal, allow double be aggregated as
        // BigDecimal during cube build for best precision
        if ("double".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).doubleValue();
        } else if ("integer".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).intValue();
        } else if ("float".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).floatValue();
        } else if ("date".equals(dataType) && fieldValue instanceof Long) {
            long millis = ((Long)fieldValue).longValue();
            fieldValue = (int) (millis / (1000 * 3600 * 24));
        } else if ("smallint".equals(dataType) && fieldValue instanceof Long) {
            fieldValue = ((Long)fieldValue).shortValue();
        } else if ((!"varchar".equals(dataType) || !"char".equals(dataType)) && fieldValue instanceof String) {
            fieldValue = convertOptiqCellValue((String)fieldValue, dataType);
        }

        setFieldObjectValue(idx, fieldValue);
    }

    public void setMeasureValue(String fieldName, Object fieldValue) {
        setMeasureValue(info.getFieldIndex(fieldName), fieldValue);
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
        } else if ("float".equals(dataType)) {
            return Float.valueOf(strValue);
        } else if ("boolean".equals(dataType)) {
            return Boolean.valueOf(strValue);
        } else {
            return strValue;
        }
    }
}
