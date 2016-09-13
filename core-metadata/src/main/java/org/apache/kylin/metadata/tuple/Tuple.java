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
import java.util.List;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DoubleMutable;
import org.apache.kylin.metadata.datatype.LongMutable;
import org.apache.kylin.metadata.model.TblColRef;

import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;

/**
 * @author xjiang
 */
public class Tuple implements ITuple {

    @IgnoreSizeOf
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

    @Override
    public Object clone() {
        return makeCopy();
    }

    @Override
    public ITuple makeCopy() {
        Tuple ret = new Tuple(this.info);
        for (int i = 0; i < this.values.length; ++i) {
            ret.values[i] = this.values[i];
        }
        return ret;
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

    public String getDataTypeName(int idx) {
        return info.getDataTypeName(idx);
    }

    public void setDimensionValue(String fieldName, String fieldValue) {
        setDimensionValue(info.getFieldIndex(fieldName), fieldValue);
    }

    public void setDimensionValue(int idx, String fieldValue) {
        Object objectValue = convertOptiqCellValue(fieldValue, getDataTypeName(idx));
        values[idx] = objectValue;
    }

    public void setMeasureValue(String fieldName, Object fieldValue) {
        setMeasureValue(info.getFieldIndex(fieldName), fieldValue);
    }

    public void setMeasureValue(int idx, Object fieldValue) {
        fieldValue = convertWritableToJava(fieldValue);

        String dataType = getDataTypeName(idx);
        // special handling for BigDecimal, allow double be aggregated as
        // BigDecimal during cube build for best precision
        if ("double".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).doubleValue();
        } else if ("integer".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).intValue();
        } else if ("smallint".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).shortValue();
        } else if ("tinyint".equals(dataType)) {
            fieldValue = ((Number) fieldValue).byteValue();
        } else if ("float".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).floatValue();
        } else if ("date".equals(dataType) && fieldValue instanceof Long) {
            long millis = ((Long) fieldValue).longValue();
            fieldValue = (int) (millis / (1000 * 3600 * 24));
        } else if ("smallint".equals(dataType) && fieldValue instanceof Long) {
            fieldValue = ((Long) fieldValue).shortValue();
        } else if ((!"varchar".equals(dataType) || !"char".equals(dataType)) && fieldValue instanceof String) {
            fieldValue = convertOptiqCellValue((String) fieldValue, dataType);
        } else if ("bigint".equals(dataType) && fieldValue instanceof Double) {
            fieldValue = ((Double) fieldValue).longValue();
        }

        values[idx] = fieldValue;
    }

    private Object convertWritableToJava(Object o) {
        if (o instanceof LongMutable)
            o = ((LongMutable) o).get();
        else if (o instanceof DoubleMutable)
            o = ((DoubleMutable) o).get();
        return o;
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

    public static long getTs(ITuple row, TblColRef partitionCol) {
        //ts column type differentiate
        if (partitionCol.getDatatype().equals("date")) {
            return epicDaysToMillis(Integer.valueOf(row.getValue(partitionCol).toString()));
        } else {
            return Long.valueOf(row.getValue(partitionCol).toString());
        }
    }

    private static long epicDaysToMillis(int days) {
        return 1L * days * (1000 * 3600 * 24);
    }

    public static Object convertOptiqCellValue(String strValue, String dataTypeName) {
        if (strValue == null)
            return null;

        if ((strValue.equals("") || strValue.equals("\\N")) && !dataTypeName.equals("string"))
            return null;

        // TODO use data type enum instead of string comparison
        if ("date".equals(dataTypeName)) {
            // convert epoch time
            return Integer.valueOf(dateToEpicDays(strValue));// Optiq expects Integer instead of Long. by honma
        } else if ("timestamp".equals(dataTypeName) || "datetime".equals(dataTypeName)) {
            return Long.valueOf(DateFormat.stringToMillis(strValue));
        } else if ("tinyint".equals(dataTypeName)) {
            return Byte.valueOf(strValue);
        } else if ("smallint".equals(dataTypeName)) {
            return Short.valueOf(strValue);
        } else if ("integer".equals(dataTypeName)) {
            return Integer.valueOf(strValue);
        } else if ("bigint".equals(dataTypeName)) {
            return Long.valueOf(strValue);
        } else if ("double".equals(dataTypeName)) {
            return Double.valueOf(strValue);
        } else if ("decimal".equals(dataTypeName)) {
            return new BigDecimal(strValue);
        } else if ("float".equals(dataTypeName)) {
            return Float.valueOf(strValue);
        } else if ("boolean".equals(dataTypeName)) {
            return Boolean.valueOf(strValue);
        } else {
            return strValue;
        }
    }

    private static int dateToEpicDays(String strValue) {
        long millis = DateFormat.stringToMillis(strValue);
        return (int) (millis / (1000 * 3600 * 24));
    }

}
