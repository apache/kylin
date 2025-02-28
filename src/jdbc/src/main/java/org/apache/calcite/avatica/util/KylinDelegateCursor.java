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

package org.apache.calcite.avatica.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.avatica.ColumnMetaData;

public class KylinDelegateCursor extends AbstractCursor {

    private static final int FLOAT_TYPE_ID = 6;

    private final AbstractCursor cursor;

    public KylinDelegateCursor(AbstractCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    protected Accessor createAccessor(ColumnMetaData columnMetaData, Getter getter, Calendar localCalendar,
            ArrayImpl.Factory factory) {
        if (columnMetaData.type.id == Types.DATE && columnMetaData.type.rep == ColumnMetaData.Rep.JAVA_SQL_DATE) {
            return new KylinDateAccessor(getter);
        }
        Set<Integer> NUMBER_META_TYPE_IDS = new HashSet<>(Arrays.asList(-6, -5, 2, 3, 4, 5, 6, 7, 8));
        if (columnMetaData.type.rep == ColumnMetaData.Rep.NUMBER
                && NUMBER_META_TYPE_IDS.contains(columnMetaData.type.id)) {
            return new KylinNumberAccessor(getter, columnMetaData.scale);
        } else {
            if (columnMetaData.type.id == FLOAT_TYPE_ID) {
                // java.lang.Float.TYPE
                return new KylinFloatToDoubleAccessor(getter);
            } else {
                return cursor.createAccessor(columnMetaData, getter, localCalendar, factory);
            }
        }
    }

    private static class KylinFloatToDoubleAccessor extends AbstractCursor.AccessorImpl {
        private KylinFloatToDoubleAccessor(AbstractCursor.Getter getter) {
            super(getter);
        }

        @Override
        public double getDouble() throws SQLException {
            Object obj = this.getObject();
            if (null == obj) {
                return 0.0D;
            } else {
                if (obj instanceof Double) {
                    return (Double) obj;
                } else if (obj instanceof BigDecimal) {
                    return ((BigDecimal) obj).doubleValue();
                } else {
                    return Double.parseDouble(obj.toString());
                }
            }
        }
    }

    private static class KylinNumberAccessor extends NumberAccessor {

        KylinNumberAccessor(AbstractCursor.Getter getter, int scale) {
            super(getter, scale);
        }

        @Override
        public BigDecimal getBigDecimal(int scale) throws SQLException {
            Number n = this.getNumber();
            if (n == null) {
                return null;
            } else {
                BigDecimal decimal = AvaticaSite.toBigDecimal(n);
                return 0 != scale ? decimal.setScale(scale, RoundingMode.DOWN) : decimal;
            }
        }
    }

    @Override
    public boolean wasNull() {
        return cursor.wasNull();
    }

    @Override
    protected Getter createGetter(int ordinal) {
        return cursor.createGetter(ordinal);
    }

    @Override
    public boolean next() {
        return cursor.next();
    }

    @Override
    public void close() {
        cursor.close();
    }

}
