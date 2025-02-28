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
import java.util.Collections;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.apache.kylin.query.schema.KylinSqlValidator;

/**
 * converter that parse, validate sql and convert to relNodes
 */
public class SqlConverter {

    private final SqlParser.Config parserConfig;
    private final SqlValidator validator;
    private final SqlToRelConverter sqlToRelConverter;
    private final CalciteConnectionConfig connectionConfig;

    public SqlConverter(KylinConnectionConfig connectionConfig, RelOptPlanner planner,
            Prepare.CatalogReader catalogReader) {
        this(connectionConfig, planner, catalogReader, null);
    }

    public SqlConverter(KylinConnectionConfig connectionConfig, RelOptPlanner planner,
            Prepare.CatalogReader catalogReader, RelOptTable.ViewExpander viewExpander) {
        this.connectionConfig = connectionConfig;
        parserConfig = SqlParser.config().withQuotedCasing(connectionConfig.quotedCasing())
                .withUnquotedCasing(connectionConfig.unquotedCasing()).withQuoting(connectionConfig.quoting())
                .withIdentifierMaxLength(connectionConfig.getIdentifierMaxLength())
                .withConformance(connectionConfig.conformance()).withCaseSensitive(connectionConfig.caseSensitive());

        SqlOperatorTable sqlOperatorTable = createOperatorTable(catalogReader);
        validator = createValidator(connectionConfig, catalogReader, sqlOperatorTable);

        sqlToRelConverter = createSqlToRelConverter(connectionConfig, viewExpander, planner, validator, catalogReader);
    }

    /**
     * parse, validate and convert sql into RelNodes.
     * Note that the output relNodes are not optimized.
     */
    public RelRoot convertSqlToRelNode(String sql) throws SqlParseException {
        SqlNode sqlNode = parseSQL(sql);
        QueryContext.current().record("end_calcite_parse_sql");

        return convertToRelNode(sqlNode);
    }

    /**
     * analyze sql for views
     */
    public CalcitePrepare.AnalyzeViewResult analyzeSQl(String sql) throws SqlParseException {
        SqlNode sqlNode = parseSQL(sql);
        RelRoot relRoot = convertToRelNode(sqlNode);
        return new CalcitePrepare.AnalyzeViewResult(null, validator, sql, sqlNode, relRoot.validatedRowType, relRoot,
                null, null, null, null, false);
    }

    public static Prepare.CatalogReader createCatalogReader(CalciteConnectionConfig connectionConfig,
            CalciteSchema rootSchema, String defaultSchemaName) {
        RelDataTypeSystem relTypeSystem = new KylinRelDataTypeSystem();
        JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl(relTypeSystem);
        return new CalciteCatalogReader(rootSchema, Collections.singletonList(defaultSchemaName), javaTypeFactory,
                connectionConfig);
    }

    private SqlValidator createValidator(CalciteConnectionConfig connectionConfig, Prepare.CatalogReader catalogReader,
            SqlOperatorTable sqlOperatorTable) {
        SqlValidator.Config config = SqlValidator.Config.DEFAULT.withConformance(connectionConfig.conformance())
                .withIdentifierExpansion(true).withDefaultNullCollation(connectionConfig.defaultNullCollation());
        return new KylinSqlValidator((SqlValidatorImpl) SqlValidatorUtil.newValidator(sqlOperatorTable, catalogReader,
                javaTypeFactory(), config));
    }

    private SqlOperatorTable createOperatorTable(Prepare.CatalogReader catalogReader) {
        // see https://olapio.atlassian.net/browse/KE-42032
        // to avoid errors like "No match found for function signature xxx".
        return SqlOperatorTables.chain(
                // Add kylin udf definitions.
                catalogReader,
                // Add some spark function definitions, in the future we need define
                // SqlLibrary#KYLIN in calcite to write many function signatures.
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.SPARK),
                // Add calcite standard functions.
                SqlStdOperatorTable.instance());
    }

    private SqlToRelConverter createSqlToRelConverter(KylinConnectionConfig connectionConfig,
            RelOptTable.ViewExpander viewExpander, RelOptPlanner planner, SqlValidator sqlValidator,
            Prepare.CatalogReader catalogReader) {
        KylinConfig kylinConfig = connectionConfig.getKylinConfig();
        SqlToRelConverter.Config config = SqlToRelConverter.CONFIG //
                .withTrimUnusedFields(true) // trim unused fields
                .withInSubQueryThreshold(kylinConfig.convertInSubQueryThreshold()) // handle in-to-or
                .withExpand(Boolean.TRUE.equals(Prepare.THREAD_EXPAND.get())) //
                .addRelBuilderConfigTransform(x -> x.withBloat(kylinConfig.getProjectBloatThreshold())) // bloat
                .withExplain(false);

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
        RelNode rel = root.rel;
        if (connectionConfig.projectUnderRelRoot() && !root.isRefTrivial()) {
            final List<RexNode> projects = new ArrayList<>();
            final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            for (int field : Pair.left(root.fields)) {
                projects.add(rexBuilder.makeInputRef(rel, field));
            }
            LogicalProject project = LogicalProject.create(root.rel, ImmutableList.of(), projects,
                    root.validatedRowType);
            //RelCollation must be cleared,
            //otherwise, relRoot's top rel will be reset to LogicalSort
            //in org.apache.calcite.tools.Programs#standard's program1
            root = new RelRoot(project, root.validatedRowType, root.kind, root.fields, RelCollations.EMPTY,
                    ImmutableList.of());
        }

        return root;
    }
}
