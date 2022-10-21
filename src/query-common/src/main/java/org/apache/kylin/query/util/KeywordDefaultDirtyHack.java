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

package org.apache.kylin.query.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

public class KeywordDefaultDirtyHack implements IQueryTransformer, IPushDownConverter {

    public static String transform(String sql) {
        // KYLIN-2108, DEFAULT is hive default database, but a sql keyword too,
        // needs quote
        sql = sql.replaceAll("(?i)default\\.", "\"DEFAULT\".");
        sql = sql.replace("defaultCatalog.", "");
        sql = sql.replace("\"defaultCatalog\".", "");

        return sql;
    }

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!KylinConfig.getInstanceFromEnv().isEscapeDefaultKeywordEnabled()) {
            return sql;
        }
        return transform(sql);
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        if (!KylinConfig.getInstanceFromEnv().isEscapeDefaultKeywordEnabled()) {
            return originSql;
        }
        return transform(originSql);
    }
}
