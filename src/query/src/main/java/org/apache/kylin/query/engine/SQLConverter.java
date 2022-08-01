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

package org.apache.kylin.query.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Pair;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.schema.KylinSqlValidator;
import org.apache.kylin.query.engine.view.ModelViewExpander;

/**
 * converter that parse, validate sql and convert to relNodes
 */
public class SQLConverter {

    private final SqlParser.Config parserConfig;
    private final SqlValidator validator;
    private final SqlOperatorTable sqlOperatorTable;
    private final SqlToRelConverter sqlToRelConverter;
    private final CalciteConnectionConfig connectionConfig;

    private SQLConverter(KECalciteConfig connectionConfig, RelOptPlanner planner, Prepare.CatalogReader catalogReader) {
        this(connectionConfig, planner, catalogReader, null);
    }

    private SQLConverter(KECalciteConfig connectionConfig, RelOptPlanner planner, Prepare.CatalogReader catalogReader,
            RelOptTable.ViewExpander viewExpander) {
        this.connectionConfig = connectionConfig;
        parserConfig = SqlParser.configBuilder().setQuotedCasing(connectionConfig.quotedCasing())
                .setUnquotedCasing(connectionConfig.unquotedCasing()).setQuoting(connectionConfig.quoting())
                .setIdentifierMaxLength(connectionConfig.getIdentifierMaxLength())
                .setConformance(connectionConfig.conformance()).setCaseSensitive(connectionConfig.caseSensitive())
                .build();

        sqlOperatorTable = createOperatorTable(connectionConfig, catalogReader);
        validator = createValidator(connectionConfig, catalogReader, sqlOperatorTable);

        sqlToRelConverter = createSqlToRelConverter(viewExpander, planner, validator, catalogReader);
    }

    public static SQLConverter createConverter(KECalciteConfig connectionConfig, RelOptPlanner planner,
            Prepare.CatalogReader catalogReader) {
        // this could be a bit awkward that SQLConverter and ViewExpander seem to have a cyclical reference
        SQLConverter sqlConverter = new SQLConverter(connectionConfig, planner, catalogReader);
        return new SQLConverter(connectionConfig, planner, catalogReader,
                new ModelViewExpander(sqlConverter::convertSqlToRelNode));
    }

    /**
     * parse, validate and convert sql into RelNodes
     * Note that the output relNodes are not optimized
     * @param sql
     * @return
     * @throws SqlParseException
     */
    public RelRoot convertSqlToRelNode(String sql) throws SqlParseException {
        SqlNode sqlNode = parseSQL(sql);
        QueryContext.current().record("end calcite parse sql");

        return convertToRelNode(sqlNode);
    }

    /**
     * analyze sql for views
     * @param sql
     * @return
     * @throws SqlParseException
     */
    public CalcitePrepare.AnalyzeViewResult analyzeSQl(String sql) throws SqlParseException {
        SqlNode sqlNode = parseSQL(sql);
        RelRoot relRoot = convertToRelNode(sqlNode);
        return new CalcitePrepare.AnalyzeViewResult(null, validator, sql, sqlNode, relRoot.validatedRowType, relRoot,
                null, null, null, null, false);
    }

    private SqlValidator createValidator(CalciteConnectionConfig connectionConfig, Prepare.CatalogReader catalogReader,
            SqlOperatorTable sqlOperatorTable) {
        SqlValidator sqlValidator = new KylinSqlValidator((SqlValidatorImpl) SqlValidatorUtil
                .newValidator(sqlOperatorTable, catalogReader, javaTypeFactory(), connectionConfig.conformance()));
        sqlValidator.setIdentifierExpansion(true);
        sqlValidator.setDefaultNullCollation(connectionConfig.defaultNullCollation());
        return sqlValidator;
    }

    private SqlOperatorTable createOperatorTable(KECalciteConfig connectionConfig,
            Prepare.CatalogReader catalogReader) {
        final Collection<SqlOperatorTable> tables = new LinkedHashSet<>();
        for (String opTable : connectionConfig.operatorTables()) {
            switch (opTable) {
            case "standard":
                tables.add(SqlStdOperatorTable.instance());
                break;
            case "oracle":
                tables.add(OracleSqlOperatorTable.instance());
                break;
            case "spatial":
                tables.add(CalciteCatalogReader.operatorTable(GeoFunctions.class.getName()));
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "Unknown operator table: '%s'. Check the kylin.query.calcite.extras-props.FUN config please",
                        opTable));
            }
        }
        tables.add(SqlStdOperatorTable.instance()); // make sure the standard optable is added
        SqlOperatorTable composedOperatorTable = ChainedSqlOperatorTable.of(tables.toArray(new SqlOperatorTable[0]));
        return ChainedSqlOperatorTable.of(composedOperatorTable, // calcite optables
                catalogReader // optable for udf
        );
    }

    private SqlToRelConverter createSqlToRelConverter(RelOptTable.ViewExpander viewExpander, RelOptPlanner planner,
            SqlValidator sqlValidator, Prepare.CatalogReader catalogReader) {
        SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().withTrimUnusedFields(true)
                .withExpand(Prepare.THREAD_EXPAND.get()).withExplain(false).build();

        final RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(javaTypeFactory()));

        return new SqlToRelConverter(viewExpander, sqlValidator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, config);
    }

    private JavaTypeFactory javaTypeFactory() {
        return TypeSystem.javaTypeFactory();
    }

    private SqlNode parseSQL(String sql) throws SqlParseException {
        return SqlParser.create(sql, parserConfig).parseQuery();
    }

    private RelRoot convertToRelNode(SqlNode sqlNode) {
        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, true);

        if (connectionConfig.forceDecorrelate()) {
            root = root.withRel(sqlToRelConverter.decorrelate(sqlNode, root.rel));
        }

        /* OVERRIDE POINT */
        // https://github.com/Kyligence/KAP/issues/10964
        RelNode rel = root.rel;
        if (connectionConfig.projectUnderRelRoot() && !root.isRefTrivial()) {
            final List<RexNode> projects = new ArrayList<>();
            final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            for (int field : Pair.left(root.fields)) {
                projects.add(rexBuilder.makeInputRef(rel, field));
            }
            LogicalProject project = LogicalProject.create(root.rel, projects, root.validatedRowType);
            //RelCollation must be cleared,
            //otherwise, relRoot's top rel will be reset to LogicalSort
            //in org.apache.calcite.tools.Programs#standard's program1
            root = new RelRoot(project, root.validatedRowType, root.kind, root.fields, RelCollations.EMPTY);
        }

        return root;
    }
}
