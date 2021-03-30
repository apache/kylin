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

    public void setDimensionValueDirectly(int idx, Object objectValue) {
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
        } else if ("decimal".equals(dataType)) {
            if (fieldValue instanceof BigDecimal) {
                fieldValue = normalizeDecimal((BigDecimal) fieldValue);
            } else if (fieldValue instanceof Number) {
                fieldValue = BigDecimal.valueOf(((Number) fieldValue).doubleValue());
            }
        } else if ("float".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).floatValue();
        } else if ("integer".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).intValue();
        } else if ("bigint".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).longValue();
        } else if ("smallint".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).shortValue();
        } else if ("tinyint".equals(dataType) && fieldValue instanceof Number) {
            fieldValue = ((Number) fieldValue).byteValue();
        } else if ("date".equals(dataType) && fieldValue instanceof Long) {
            long millis = (Long) fieldValue;
            fieldValue = (int) (millis / (1000 * 3600 * 24));
        } else if ((!"varchar".equals(dataType) || !"char".equals(dataType)) && fieldValue instanceof String) {
            fieldValue = convertOptiqCellValue((String) fieldValue, dataType);
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

    private static BigDecimal normalizeDecimal(BigDecimal input) {
        if (input.scale() < 0) {
            return input.setScale(0);
        } else {
            return input;
        }
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
            return epicDaysToMillis(Integer.parseInt(row.getValue(partitionCol).toString()));
        } else {
            return Long.parseLong(row.getValue(partitionCol).toString());
        }
    }

    public static long epicDaysToMillis(int days) {
        return 1L * days * (1000 * 3600 * 24);
    }

    public static int millisToEpicDays(long millis) {
        return (int) (millis / (1000 * 3600 * 24));
    }

    public static Object convertOptiqCellValue(String strValue, String dataTypeName) {
        if (strValue == null)
            return null;

        if ((strValue.equals("") || strValue.equals("\\N")) && !dataTypeName.equals("string")
                && !dataTypeName.startsWith("varchar"))
            return null;

        switch (dataTypeName) {
        case "date":
            // convert epoch time
            return millisToEpicDays(DateFormat.stringToMillis(strValue));// Optiq expects Integer instead of Long. by honma
        case "datetime":
        case "timestamp":
            return DateFormat.stringToMillis(strValue);
        case "tinyint":
            return Byte.valueOf(strValue);
        case "smallint":
            return Short.valueOf(strValue);
        case "integer":
            return Integer.valueOf(strValue);
        case "bigint":
            return Long.valueOf(strValue);
        case "double":
            return Double.valueOf(strValue);
        case "decimal":
            return normalizeDecimal(new BigDecimal(strValue));
        case "float":
            return Float.valueOf(strValue);
        case "boolean":
            return Boolean.valueOf(strValue) || "1".equals(strValue); // in some extended encodings boolean might be encoded as a number
        default:
            return strValue;
        }
    }

}
