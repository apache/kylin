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
package org.apache.kylin.common.persistence.metadata.mapper;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.apache.kylin.common.KylinConfig;
import org.mybatis.dynamic.sql.AliasableSqlTable;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlColumn;

import lombok.Getter;

/**
 * 16/11/2023 hellozepp(lisheng.zhanglin@163.com)
 */
@Getter
public class BasicSqlTable<T extends AliasableSqlTable<T>> extends AliasableSqlTable<T> {
    public static final String CONTENT_FIELD = "content";

    public static final String ID_FIELD = "id";

    public static final String UUID_FIELD = "uuid";

    public static final String MVCC_FIELD = "mvcc";

    public static final String TS_FIELD = "ts";

    public static final String PROJECT_FIELD = "project";

    public static final String META_KEY_FIELD = "meta_key";

    public static final String META_KEY_PROPERTIES_NAME = "metaKey";

    public final SqlColumn<Long> id = column(ID_FIELD, JDBCType.BIGINT);

    public final SqlColumn<String> uuid = column(UUID_FIELD, JDBCType.CHAR);

    public final SqlColumn<Long> mvcc = column(MVCC_FIELD, JDBCType.BIGINT);

    public final SqlColumn<Long> ts = column(TS_FIELD, JDBCType.BIGINT);

    public final SqlColumn<String> project = column(PROJECT_FIELD, JDBCType.VARCHAR);

    public final SqlColumn<String> metaKey = column(META_KEY_FIELD, JDBCType.VARCHAR);

    public final SqlColumn<byte[]> content = column(CONTENT_FIELD, JDBCType.LONGVARBINARY);

    public final SqlColumn<String> reservedFiled1 = column("reserved_filed_1", JDBCType.VARCHAR);

    public final SqlColumn<byte[]> reservedFiled2 = column("reserved_filed_2", JDBCType.LONGVARBINARY);

    public final SqlColumn<byte[]> reservedFiled3 = column("reserved_filed_3", JDBCType.LONGVARBINARY);

    private final String tableNameSuffix;
    private String tableNamePrefix;

    public BasicSqlTable(String tableNameSuffix, Supplier<T> constructor) {
        super(KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier() + "_" + tableNameSuffix, constructor);
        this.tableNameSuffix = tableNameSuffix;
        this.tableNamePrefix = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
    }

    public List<BasicColumn> getGeneralColumns() {
        return new ArrayList<>(
                Arrays.asList(id, uuid, mvcc, ts, metaKey, content, reservedFiled1, reservedFiled2, reservedFiled3));
    }

    public void updateTableName() {
        this.tableNamePrefix = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        this.nameSupplier = () -> tableNamePrefix + "_" + tableNameSuffix;
    }

}
