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
package org.apache.kylin.common.persistence.metadata.jdbc;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.ByteArrayTypeHandler;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.CompressionUtils;

/**
 * 15/11/2023 hellozepp(lisheng.zhanglin@163.com)
 */
public class ContentTypeHandler extends ByteArrayTypeHandler {

    @Override
    public byte[] getNullableResult(ResultSet rs, String columnName) throws SQLException {
        byte[] s = super.getNullableResult(rs, columnName);
        try {
            return CompressionUtils.decompress(s);
        } catch (Exception e) {
            throw new KylinRuntimeException("Can not compression the content!", e);
        }
    }

    @Override
    public byte[] getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        byte[] s = super.getNullableResult(rs, columnIndex);
        try {
            return CompressionUtils.decompress(s);
        } catch (Exception e) {
            throw new KylinRuntimeException("Can not compression the content!", e);
        }
    }

    @Override
    public byte[] getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        byte[] s = super.getNullableResult(cs, columnIndex);
        try {
            return CompressionUtils.decompress(s);
        } catch (Exception e) {
            throw new KylinRuntimeException("Can not compression the content!", e);
        }
    }
}
