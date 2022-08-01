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

package org.apache.kylin.query.calcite;

import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.Frameworks;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ValidatorTest {
    SqlValidator validator;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws SqlParseException {
        RelDataTypeFactory factory = new SqlTypeFactoryImpl(new KylinRelDataTypeSystem());
        SchemaPlus rootScheme = Frameworks.createRootSchema(true);
        rootScheme.add("PEOPLE", new AbstractTable() { //note: add a table
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                builder.add("ID", factory.createSqlType(SqlTypeName.INTEGER));
                builder.add("NAME", factory.createSqlType(SqlTypeName.VARCHAR));
                builder.add("AGE", factory.createSqlType(SqlTypeName.INTEGER));
                builder.add("BIRTHDAY", factory.createSqlType(SqlTypeName.DATE));
                return builder.build();
            }
        });
        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(CalciteSchema.from(rootScheme),
                CalciteSchema.from(rootScheme).path(null), factory, new CalciteConnectionConfigImpl(new Properties()));
        validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader, factory);
    }

    @Test
    public void testImplicitTypeCast() {
        try {
            validator.validate(parse("select sum(AGE + NAME) from PEOPLE "));
            validator.validate(parse("select sum(AGE - NAME) from PEOPLE "));
            validator.validate(parse("select sum(AGE * NAME) from PEOPLE "));
            validator.validate(parse("select sum(AGE / NAME) from PEOPLE "));
            validator.validate(parse("select * from PEOPLE where BIRTHDAY = cast('2013-01-01 00:00:01' as timestamp)"));
        } catch (Exception e) {
            fail("Should not have thrown any exception");
        }
    }

    private SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        return parser.parseQuery();
    }

}
