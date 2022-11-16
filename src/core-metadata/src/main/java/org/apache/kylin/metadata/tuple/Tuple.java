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

    public void setDimensionValue(int idx, String fieldValue) {
        Object objectValue = convertOptiqCellValue(fieldValue, getDataTypeName(idx));
        values[idx] = objectValue;
    }

    public void setMeasureValue(int idx, Object fieldValue) {
        fieldValue = convertWritableToJava(fieldValue);

        String dataType = getDataTypeName(idx);
        // special handling for BigDecimal, allow double be aggregated as
        // BigDecimal during cube build for best precision
        if ("double".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = ((BigDecimal) fieldValue).doubleValue();
        } else if ("decimal".equals(dataType) && fieldValue instanceof BigDecimal) {
            fieldValue = normalizeDecimal((BigDecimal) fieldValue);
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

    private static BigDecimal normalizeDecimal(BigDecimal input) {
        if (input.scale() < 0) {
            return input.setScale(0);
        } else {
            return input;
        }
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

    public static Object convertOptiqCellValue(String strValue, String dataTypeName) {
        if (strValue == null)
            return null;

        if ((strValue.equals("") || strValue.equals("\\N")) && !dataTypeName.equals("string")
                && !dataTypeName.startsWith("varchar"))
            return null;

        switch (dataTypeName) {
        case "date":
            // convert epoch time
            return dateToEpicDays(strValue);// Optiq expects Integer instead of Long. by honma
        case "datetime":
        case "timestamp":
            return DateFormat.stringToMillis(strValue);
        case "tinyint":
            return Byte.parseByte(strValue);
        case "smallint":
            return Short.parseShort(strValue);
        case "integer":
            return Integer.parseInt(strValue);
        case "bigint":
            return Long.parseLong(strValue);
        case "double":
            return Double.parseDouble(strValue);
        case "decimal":
            return normalizeDecimal(new BigDecimal(strValue));
        case "float":
            return Float.parseFloat(strValue);
        case "boolean":
            return Boolean.parseBoolean(strValue) || "1".equals(strValue); // in some extended encodings boolean might be encoded as a number
        default:
            return strValue;
        }
    }

    private static int dateToEpicDays(String strValue) {
        long millis = DateFormat.stringToMillis(strValue);
        return (int) (millis / (1000 * 3600 * 24));
    }

}
