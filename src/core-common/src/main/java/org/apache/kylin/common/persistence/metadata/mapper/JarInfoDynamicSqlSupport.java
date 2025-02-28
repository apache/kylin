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

import org.mybatis.dynamic.sql.SqlColumn;

public final class JarInfoDynamicSqlSupport {

    public static final JarInfo sqlTable = new JarInfo();

    private JarInfoDynamicSqlSupport() {
    }

    public static final class JarInfo extends BasicSqlTable<JarInfo> {
        public final SqlColumn<String> jarType = column("jar_type", JDBCType.VARCHAR);

        public final SqlColumn<String> jarName = column("jar_name", JDBCType.VARCHAR);

        public JarInfo() {
            super("jar_info", JarInfo::new);
        }
    }
}
