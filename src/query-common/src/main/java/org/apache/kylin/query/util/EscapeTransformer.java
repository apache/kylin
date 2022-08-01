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

import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EscapeTransformer implements KapQueryUtil.IQueryTransformer {

    private static final Logger logger = LoggerFactory.getLogger(EscapeTransformer.class);

    private EscapeDialect dialect = EscapeDialect.DEFAULT;

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!KapConfig.getInstanceFromEnv().isJdbcEscapeEnabled()) {
            return sql;
        }

        return transform(sql);
    }

    public String transform(String sql) {
        try {
            // remove the comment
            sql = new RawSqlParser(sql).parse().getStatementString();
        } catch (Throwable ex) {
            logger.error("Something unexpected while CommentParser transforming the query, return original query", ex);
            logger.error(sql);
        }
        try {
            EscapeParser parser = new EscapeParser(dialect, sql);
            String result = parser.Input();
            logger.debug("EscapeParser done parsing");
            return result;
        } catch (Throwable ex) {
            logger.error("Something unexpected while EscapeTransformer transforming the query, return original query", ex);
            logger.error(sql);
            return sql;
        }
    }

    public void setFunctionDialect(EscapeDialect newDialect) {
        this.dialect = newDialect;
    }
}
