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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SingleSqlNodeReplacerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testReplace() throws SqlParseException {
        ConvMaster convMaster = new ConvMaster(null, null);
        SingleSqlNodeReplacer replacer = new SingleSqlNodeReplacer(convMaster);

        SqlNode origin = SqlParser.create("1 = 1 and (a = 'a' or  a = 'aa')").parseExpression();
        SqlNode sqlNodeTryToFind = SqlParser.create("a").parseExpression();
        SqlNode sqlNodeToReplace = SqlParser.create("to_char(a,'yyyy')").parseExpression();

        replacer.setSqlNodeTryToFind(sqlNodeTryToFind);
        replacer.setSqlNodeToReplace(sqlNodeToReplace);

        SqlNode replaced = origin.accept(replacer);

        SqlPrettyWriter writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
        String sqlReplace = writer.format(replaced);

        Assert.assertEquals("1 = 1 AND (TO_CHAR(\"A\", 'yyyy') = 'a' OR TO_CHAR(\"A\", 'yyyy') = 'aa')", sqlReplace);

        origin = SqlParser.create("aa = 1 and (a = 'a' or  a = 'aa')").parseExpression();
        replaced = origin.accept(replacer);
        writer.reset();
        sqlReplace = writer.format(replaced);

        Assert.assertEquals("\"AA\" = 1 AND (TO_CHAR(\"A\", 'yyyy') = 'a' OR TO_CHAR(\"A\", 'yyyy') = 'aa')",
                sqlReplace);
    }

}
