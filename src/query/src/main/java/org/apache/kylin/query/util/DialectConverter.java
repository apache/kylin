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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.calcite.KEDialect;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DialectConverter implements IPushDownConverter {

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        try {
            SqlNode node = CalciteParser.parse(originSql, project);
            SqlPrettyWriter writer = new SqlPrettyWriter(KEDialect.DEFAULT);
            return writer.format(node);
        } catch (Exception e) {
            log.error("dialect push down converter failed.", e);
        }
        return originSql;
    }
}
