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
import java.util.Calendar;

import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.avatica.ColumnMetaData;

public class KylinDelegateCursor extends AbstractCursor {

    private final AbstractCursor cursor;

    public KylinDelegateCursor(AbstractCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    protected Accessor createAccessor(ColumnMetaData columnMetaData, Getter getter, Calendar localCalendar, ArrayImpl.Factory factory) {
        if (columnMetaData.type.id == Types.DATE && columnMetaData.type.rep == ColumnMetaData.Rep.JAVA_SQL_DATE) {
            return new KylinDateAccessor(getter);
        }
        switch(columnMetaData.type.rep) {
            case NUMBER:
                switch (columnMetaData.type.id) {
                    case -6:
                    case -5:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                        return new KylinNumberAccessor(getter, columnMetaData.scale);
                }
            default:
                switch (columnMetaData.type.id) {
                    case 6:    // java.lang.Float.TYPE
                        return new KylinFloatToDoubleAccessor(getter);
                    default:
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
                    return (Double)obj;
                } else if (obj instanceof BigDecimal) {
                    return ((BigDecimal)obj).doubleValue();
                } else {
                    return Double.valueOf(obj.toString());
                }
            }
        }
    }

    private class KylinNumberAccessor extends NumberAccessor {

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
