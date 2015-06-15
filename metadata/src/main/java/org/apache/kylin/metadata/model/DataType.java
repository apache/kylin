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

package org.apache.kylin.metadata.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yangli9
 * 
 */
public class DataType {
    
    public static final String VALID_TYPES_STRING = "any|char|varchar|boolean|binary" //
            + "|integer|tinyint|smallint|bigint|decimal|numeric|float|real|double" //
            + "|date|time|datetime|timestamp|byte|int|short|long|string|hllc" //
            + "|" + TblColRef.InnerDataTypeEnum.LITERAL.getDataType() //
            + "|" + TblColRef.InnerDataTypeEnum.DERIVED.getDataType();

    private static final Pattern TYPE_PATTERN = Pattern.compile(
    // standard sql types, ref:
    // http://www.w3schools.com/sql/sql_datatypes_general.asp
            "(" + VALID_TYPES_STRING + ")" + "\\s*" //
                    + "(?:" + "[(]" + "([\\d\\s,]+)" + "[)]" + ")?", Pattern.CASE_INSENSITIVE);

    public static final Set<String> INTEGER_FAMILY = new HashSet<String>();
    public static final Set<String> NUMBER_FAMILY = new HashSet<String>();
    public static final Set<String> DATETIME_FAMILY = new HashSet<String>();
    public static final Set<String> STRING_FAMILY = new HashSet<String>();
    private static final Set<Integer> HLLC_PRECISIONS = new HashSet<Integer>();
    private static final Map<String, String> LEGACY_TYPE_MAP = new HashMap<String, String>();
    static {
        INTEGER_FAMILY.add("tinyint");
        INTEGER_FAMILY.add("smallint");
        INTEGER_FAMILY.add("integer");
        INTEGER_FAMILY.add("bigint");

        NUMBER_FAMILY.addAll(INTEGER_FAMILY);
        NUMBER_FAMILY.add("float");
        NUMBER_FAMILY.add("double");
        NUMBER_FAMILY.add("decimal");
        NUMBER_FAMILY.add("real");
        NUMBER_FAMILY.add("numeric");

        DATETIME_FAMILY.add("date");
        DATETIME_FAMILY.add("time");
        DATETIME_FAMILY.add("datetime");
        DATETIME_FAMILY.add("timestamp");

        STRING_FAMILY.add("varchar");
        STRING_FAMILY.add("char");

        LEGACY_TYPE_MAP.put("byte", "tinyint");
        LEGACY_TYPE_MAP.put("int", "integer");
        LEGACY_TYPE_MAP.put("short", "smallint");
        LEGACY_TYPE_MAP.put("long", "bigint");
        LEGACY_TYPE_MAP.put("string", "varchar");
        LEGACY_TYPE_MAP.put("hllc10", "hllc(10)");
        LEGACY_TYPE_MAP.put("hllc12", "hllc(12)");
        LEGACY_TYPE_MAP.put("hllc14", "hllc(14)");
        LEGACY_TYPE_MAP.put("hllc15", "hllc(15)");
        LEGACY_TYPE_MAP.put("hllc16", "hllc(16)");

        for (int i = 10; i <= 16; i++)
            HLLC_PRECISIONS.add(i);
    }

    private static final ConcurrentMap<DataType, DataType> CACHE = new ConcurrentHashMap<DataType, DataType>();

    public static final DataType ANY = DataType.getInstance("any");

    public static DataType getInstance(String type) {
        if (type == null)
            return null;

        DataType dataType = new DataType(type);
        DataType cached = CACHE.get(dataType);
        if (cached == null) {
            CACHE.put(dataType, dataType);
            cached = dataType;
        }
        return cached;
    }

    // ============================================================================

    private String name;
    private int precision;
    private int scale;

    DataType(String datatype) {
        parseDataType(datatype);
    }

    private void parseDataType(String datatype) {
        datatype = datatype.trim().toLowerCase();
        datatype = replaceLegacy(datatype);

        Matcher m = TYPE_PATTERN.matcher(datatype);
        if (m.matches() == false)
            throw new IllegalArgumentException("bad data type -- " + datatype + ", does not match " + TYPE_PATTERN);

        name = replaceLegacy(m.group(1));
        precision = -1;
        scale = -1;

        String leftover = m.group(2);
        if (leftover != null) {
            String[] parts = leftover.split("\\s*,\\s*");
            for (int i = 0; i < parts.length; i++) {
                int n;
                try {
                    n = Integer.parseInt(parts[i]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("bad data type -- " + datatype + ", precision/scale not numeric");
                }
                if (i == 0)
                    precision = n;
                else if (i == 1)
                    scale = n;
                else
                    throw new IllegalArgumentException("bad data type -- " + datatype + ", too many precision/scale parts");
            }
        }


        // FIXME 256 for unknown string precision
        if ((name.equals("char") || name.equals("varchar")) && precision == -1) {
            precision = 256; // to save memory at frontend, e.g. tableau will
                             // allocate memory according to this
        }

        // FIXME (19,4) for unknown decimal precision
        if ((name.equals("decimal") || name.equals("numeric")) && precision == -1) {
            precision = 39;
            scale = 16;
        }

        if (isHLLC() && HLLC_PRECISIONS.contains(precision) == false)
            throw new IllegalArgumentException("HLLC precision must be one of " + HLLC_PRECISIONS);
    }

    private String replaceLegacy(String str) {
        String replace = LEGACY_TYPE_MAP.get(str);
        return replace == null ? str : replace;
    }

    public int getSpaceEstimate() {
        if (isTinyInt()) {
            return 1;
        } else if (isSmallInt()) {
            return 2;
        } else if (isInt()) {
            return 4;
        } else if (isBigInt()) {
            return 8;
        } else if (isFloat()) {
            return 4;
        } else if (isDouble()) {
            return 8;
        } else if (isDecimal()) {
            return 8;
        } else if (isHLLC()) {
            return 1 << precision;
        }
        throw new IllegalStateException("The return type : " + name + " is not recognized;");
    }

    public boolean isStringFamily() {
        return STRING_FAMILY.contains(name);
    }

    public boolean isIntegerFamily() {
        return INTEGER_FAMILY.contains(name);
    }

    public boolean isNumberFamily() {
        return NUMBER_FAMILY.contains(name);
    }

    public boolean isDateTimeFamily() {
        return DATETIME_FAMILY.contains(name);
    }

    public boolean isTinyInt() {
        return name.equals("tinyint");
    }

    public boolean isSmallInt() {
        return name.equals("smallint");
    }

    public boolean isInt() {
        return name.equals("integer");
    }

    public boolean isBigInt() {
        return name.equals("bigint");
    }

    public boolean isFloat() {
        return name.equals("float");
    }

    public boolean isDouble() {
        return name.equals("double");
    }

    public boolean isDecimal() {
        return name.equals("decimal");
    }

    public boolean isHLLC() {
        return name.equals("hllc");
    }

    public String getName() {
        return name;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + precision;
        result = prime * result + scale;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataType other = (DataType) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (precision != other.precision)
            return false;
        if (scale != other.scale)
            return false;
        return true;
    }

    @Override
    public String toString() {
        if (precision < 0 && scale < 0)
            return name;
        else if (scale < 0)
            return name + "(" + precision + ")";
        else
            return name + "(" + precision + "," + scale + ")";
    }
}
