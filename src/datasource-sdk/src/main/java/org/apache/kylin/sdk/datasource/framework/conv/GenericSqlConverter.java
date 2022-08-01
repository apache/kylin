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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDefProvider;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class GenericSqlConverter {

    private final Cache<String, SqlConverter> SQL_CONVERTER_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS).maximumSize(30).build();

    public String convertSql(String originSql, String sourceDialect, String targetDialect) throws SQLException {
        SqlConverter sqlConverter = getSqlConverter(sourceDialect, targetDialect);
        return sqlConverter.convertSql(originSql);
    }

    private SqlConverter getSqlConverter(String sourceDialect, String targetDialect) throws SQLException {
        String cacheKey = sourceDialect + "_" + targetDialect;
        SqlConverter sqlConverter = SQL_CONVERTER_CACHE.getIfPresent(cacheKey);
        if (sqlConverter == null) {
            sqlConverter = createSqlConverter(sourceDialect, targetDialect);
            SQL_CONVERTER_CACHE.put(cacheKey, sqlConverter);
        }
        return sqlConverter;
    }

    private SqlConverter createSqlConverter(String sourceDialect, String targetDialect) throws SQLException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        DataSourceDef sourceDs = provider.getById(sourceDialect);
        final DataSourceDef targetDs = provider.getById(targetDialect);
        ConvMaster convMaster = new ConvMaster(sourceDs, targetDs);
        SqlConverter.IConfigurer configurer = new DefaultConfigurer(targetDs);
        return new SqlConverter(configurer, convMaster);
    }
}
