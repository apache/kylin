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

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.DateTimeUtils;

/**
 * a mirror of org.apache.calcite.avatica.util.AbstractCursor.DateAccessor with the getTimestamp method impl
 * org.apache.calcite.avatica.util.KylinDateAccessor is for the compatibility of Oracle BIP which always use getTimestamp method to fetch Date typed data
 */
public class KylinDateAccessor extends AbstractCursor.AccessorImpl {

    public KylinDateAccessor(AbstractCursor.Getter getter) {
        super(getter);
    }

    @Override
    public Date getDate(Calendar calendar) throws SQLException {
        Date date = (Date) getObject();
        if (date == null) {
            return null;
        }
        if (calendar != null) {
            long v = date.getTime();
            v -= calendar.getTimeZone().getOffset(v);
            date = new Date(v);
        }
        return date;
    }

    @Override
    public Timestamp getTimestamp(Calendar calendar) throws SQLException {
        Date date = getDate(calendar);
        if (date == null) {
            return null;
        }
        return new Timestamp(date.getTime());
    }

    @Override
    public String getString() throws SQLException {
        final int v = getInt();
        if (v == 0 && wasNull()) {
            return null;
        }
        return dateAsString(v, null);
    }

    @Override
    public long getLong() throws SQLException {
        Date date = getDate(null);
        return date == null ? 0L : (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
    }

    private static String dateAsString(int v, Calendar calendar) {
        AvaticaUtils.discard(calendar); // time zone shift doesn't make sense
        return DateTimeUtils.unixDateToString(v);
    }
}
