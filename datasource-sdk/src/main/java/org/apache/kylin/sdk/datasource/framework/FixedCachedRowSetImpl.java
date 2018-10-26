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
package org.apache.kylin.sdk.datasource.framework;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Ref;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Hashtable;

import javax.sql.rowset.RowSetMetaDataImpl;

import com.sun.rowset.CachedRowSetImpl;

/**
 * Fixed a bug in Oracle JDK: https://stackoverflow.com/questions/15184709/cachedrowsetimpl-getstring-based-on-column-label
 */
public class FixedCachedRowSetImpl extends CachedRowSetImpl {

    private static final long serialVersionUID = -9067504047398250113L;
    private RowSetMetaDataImpl rowSetMD;

    public FixedCachedRowSetImpl() throws SQLException {
        super();
    }

    public FixedCachedRowSetImpl(Hashtable env) throws SQLException {
        super(env);
    }

    private int getColIdxByName(String name) throws SQLException {
        rowSetMD = (RowSetMetaDataImpl) this.getMetaData();
        int cols = rowSetMD.getColumnCount();

        for (int i = 1; i <= cols; ++i) {
            String colName = rowSetMD.getColumnLabel(i);
            if (colName != null)
                if (name.equalsIgnoreCase(colName))
                    return (i);
                else
                    continue;
        }
        throw new SQLException(resBundle.handleGetObject("cachedrowsetimpl.invalcolnm").toString());
    }

    @Override
    public Collection<?> toCollection(String column) throws SQLException {
        return toCollection(getColIdxByName(column));
    }

    @Override
    public String getString(String columnName) throws SQLException {
        return getString(getColIdxByName(columnName));
    }

    @Override
    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(getColIdxByName(columnName));
    }

    @Override
    public byte getByte(String columnName) throws SQLException {
        return getByte(getColIdxByName(columnName));
    }

    @Override
    public short getShort(String columnName) throws SQLException {
        return getShort(getColIdxByName(columnName));
    }

    @Override
    public int getInt(String columnName) throws SQLException {
        return getInt(getColIdxByName(columnName));
    }

    @Override
    public long getLong(String columnName) throws SQLException {
        return getLong(getColIdxByName(columnName));
    }

    @Override
    public float getFloat(String columnName) throws SQLException {
        return getFloat(getColIdxByName(columnName));
    }

    @Override
    public double getDouble(String columnName) throws SQLException {
        return getDouble(getColIdxByName(columnName));
    }

    @Override
    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(getColIdxByName(columnName));
    }

    @Override
    public java.sql.Date getDate(String columnName) throws SQLException {
        return getDate(getColIdxByName(columnName));
    }

    @Override
    public java.sql.Time getTime(String columnName) throws SQLException {
        return getTime(getColIdxByName(columnName));
    }

    @Override
    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(getColIdxByName(columnName));
    }

    @Override
    public java.io.InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream(getColIdxByName(columnName));

    }

    @Override
    public java.io.InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(getColIdxByName(columnName));
    }

    @Override
    public Object getObject(String columnName) throws SQLException {
        return getObject(getColIdxByName(columnName));
    }

    @Override
    public int findColumn(String columnName) throws SQLException {
        return getColIdxByName(columnName);
    }

    @Override
    public java.io.Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream(getColIdxByName(columnName));
    }

    @Override
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(getColIdxByName(columnName));
    }

    @Override
    public boolean columnUpdated(String columnName) throws SQLException {
        return columnUpdated(getColIdxByName(columnName));
    }

    @Override
    public void updateNull(String columnName) throws SQLException {
        updateNull(getColIdxByName(columnName));
    }

    @Override
    public void updateBoolean(String columnName, boolean x) throws SQLException {
        updateBoolean(getColIdxByName(columnName), x);
    }

    @Override
    public void updateByte(String columnName, byte x) throws SQLException {
        updateByte(getColIdxByName(columnName), x);
    }

    @Override
    public void updateShort(String columnName, short x) throws SQLException {
        updateShort(getColIdxByName(columnName), x);
    }

    @Override
    public void updateInt(String columnName, int x) throws SQLException {
        updateInt(getColIdxByName(columnName), x);
    }

    @Override
    public void updateLong(String columnName, long x) throws SQLException {
        updateLong(getColIdxByName(columnName), x);
    }

    @Override
    public void updateFloat(String columnName, float x) throws SQLException {
        updateFloat(getColIdxByName(columnName), x);
    }

    @Override
    public void updateDouble(String columnName, double x) throws SQLException {
        updateDouble(getColIdxByName(columnName), x);
    }

    @Override
    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        updateBigDecimal(getColIdxByName(columnName), x);
    }

    @Override
    public void updateString(String columnName, String x) throws SQLException {
        updateString(getColIdxByName(columnName), x);
    }

    @Override
    public void updateBytes(String columnName, byte[] x) throws SQLException {
        updateBytes(getColIdxByName(columnName), x);
    }

    @Override
    public void updateDate(String columnName, java.sql.Date x) throws SQLException {
        updateDate(getColIdxByName(columnName), x);
    }

    @Override
    public void updateTime(String columnName, java.sql.Time x) throws SQLException {
        updateTime(getColIdxByName(columnName), x);
    }

    @Override
    public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException {
        updateTimestamp(getColIdxByName(columnName), x);
    }

    @Override
    public void updateAsciiStream(String columnName, java.io.InputStream x, int length) throws SQLException {
        updateAsciiStream(getColIdxByName(columnName), x, length);
    }

    @Override
    public void updateBinaryStream(String columnName, java.io.InputStream x, int length) throws SQLException {
        updateBinaryStream(getColIdxByName(columnName), x, length);
    }

    @Override
    public void updateCharacterStream(String columnName, java.io.Reader reader, int length) throws SQLException {
        updateCharacterStream(getColIdxByName(columnName), reader, length);
    }

    @Override
    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        updateObject(getColIdxByName(columnName), x, scale);
    }

    @Override
    public void updateObject(String columnName, Object x) throws SQLException {
        updateObject(getColIdxByName(columnName), x);
    }

    @Override
    public Object getObject(String columnName, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(getColIdxByName(columnName), map);
    }

    @Override
    public Ref getRef(String colName) throws SQLException {
        return getRef(getColIdxByName(colName));
    }

    @Override
    public Blob getBlob(String colName) throws SQLException {
        return getBlob(getColIdxByName(colName));
    }

    @Override
    public Clob getClob(String colName) throws SQLException {
        return getClob(getColIdxByName(colName));
    }

    @Override
    public Array getArray(String colName) throws SQLException {
        return getArray(getColIdxByName(colName));
    }

    @Override
    public java.sql.Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate(getColIdxByName(columnName), cal);
    }

    @Override
    public java.sql.Time getTime(String columnName, Calendar cal) throws SQLException {
        return getTime(getColIdxByName(columnName), cal);
    }

    @Override
    public java.sql.Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        return getTimestamp(getColIdxByName(columnName), cal);
    }

    @Override
    public void updateRef(String columnName, java.sql.Ref ref) throws SQLException {
        updateRef(getColIdxByName(columnName), ref);
    }

    @Override
    public void updateClob(String columnName, Clob c) throws SQLException {
        updateClob(getColIdxByName(columnName), c);
    }

    @Override
    public void updateBlob(String columnName, Blob b) throws SQLException {
        updateBlob(getColIdxByName(columnName), b);
    }

    @Override
    public void updateArray(String columnName, Array a) throws SQLException {
        updateArray(getColIdxByName(columnName), a);
    }

    @Override
    public java.net.URL getURL(String columnName) throws SQLException {
        return getURL(getColIdxByName(columnName));
    }
}