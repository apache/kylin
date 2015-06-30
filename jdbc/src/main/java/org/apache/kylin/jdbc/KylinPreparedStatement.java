package org.apache.kylin.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;

public class KylinPreparedStatement extends AvaticaPreparedStatement {

    protected KylinPreparedStatement(AvaticaConnection connection, StatementHandle h, Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        super(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
    
    protected List<Object> getParameterValues() {
        return Arrays.asList(slots);
    }
    
    // ============================================================================

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        getParameter(parameterIndex).setRowId(slots, parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        getParameter(parameterIndex).setNString(slots, parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        getParameter(parameterIndex).setNCharacterStream(slots, parameterIndex, value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        getParameter(parameterIndex).setNClob(slots, parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getParameter(parameterIndex).setClob(slots, parameterIndex, reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        getParameter(parameterIndex).setBlob(slots, parameterIndex, inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getParameter(parameterIndex).setNClob(slots, parameterIndex, reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        getParameter(parameterIndex).setSQLXML(slots, parameterIndex, xmlObject);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getParameter(parameterIndex).setAsciiStream(slots, parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getParameter(parameterIndex).setBinaryStream(slots, parameterIndex, x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        getParameter(parameterIndex).setCharacterStream(slots, parameterIndex, reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        getParameter(parameterIndex).setAsciiStream(slots, parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        getParameter(parameterIndex).setBinaryStream(slots, parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        getParameter(parameterIndex).setCharacterStream(slots, parameterIndex, reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        getParameter(parameterIndex).setNCharacterStream(slots, parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        getParameter(parameterIndex).setClob(slots, parameterIndex, reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        getParameter(parameterIndex).setBlob(slots, parameterIndex, inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        getParameter(parameterIndex).setNClob(slots, parameterIndex, reader);
    }
}
