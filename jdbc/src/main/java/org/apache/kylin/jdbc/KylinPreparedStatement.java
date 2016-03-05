package org.apache.kylin.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;

public class KylinPreparedStatement extends AvaticaPreparedStatement {

    protected KylinPreparedStatement(AvaticaConnection connection, StatementHandle h, Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        super(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
        if (this.handle.signature == null)
            this.handle.signature = signature;
    }

    protected List<Object> getParameterValues2() {
        List<Object> values = new ArrayList<>(slots.length);
        for (int i = 0; i < slots.length; i++) {
            values.add(slots[i].value);
        }
        return values;
    }

    // ============================================================================

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        getSite(parameterIndex).setRowId(x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        getSite(parameterIndex).setNString(value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        getSite(parameterIndex).setNCharacterStream(value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        getSite(parameterIndex).setNClob(value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getSite(parameterIndex).setClob(reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        getSite(parameterIndex).setBlob(inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getSite(parameterIndex).setNClob(reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        getSite(parameterIndex).setSQLXML(xmlObject);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getSite(parameterIndex).setAsciiStream(x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getSite(parameterIndex).setBinaryStream(x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        getSite(parameterIndex).setCharacterStream(reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        getSite(parameterIndex).setAsciiStream(x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        getSite(parameterIndex).setBinaryStream(x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        getSite(parameterIndex).setCharacterStream(reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        getSite(parameterIndex).setNCharacterStream(value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        getSite(parameterIndex).setClob(reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        getSite(parameterIndex).setBlob(inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        getSite(parameterIndex).setNClob(reader);
    }
}
