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

package org.apache.kylin.query.engine.view;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.metadata.model.NDataModel;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

/**
 * A schema with no concrete tables
 * view tables will be registered later and accessed via
 * org.apache.calcite.jdbc.CalciteSchema#getTableBasedOnNullaryFunction(java.lang.String, boolean)
 */
public class ViewSchema extends AbstractSchema {

    private final String schemaName;
    private final ViewAnalyzer analyzer;

    public ViewSchema(String schemaName, ViewAnalyzer analyzer) {
        this.schemaName = schemaName;
        this.analyzer = analyzer;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return new HashMap<>();
    }

    public void addModel(SchemaPlus schemaPlus, NDataModel model) {
        schemaPlus.add(model.getAlias(), createViewMacro(model));
    }

    private LazyParsedViewTableMacro createViewMacro(NDataModel model) {
        String viewSQL = new ModelViewGenerator(model).generateViewSQL();
        List<String> schemaPath = Lists.newArrayList(schemaName);
        List<String> viewPath = Lists.newArrayList(schemaName, model.getAlias());
        return new LazyParsedViewTableMacro(() -> analyzeView(viewSQL), viewSQL, schemaPath, viewPath);
    }

    public CalcitePrepare.AnalyzeViewResult analyzeView(String sql) {
        try {
            return analyzer.analyzeView(sql);
        } catch (SqlParseException e) {
            throw new KylinException(QueryErrorCode.FAILED_PARSE_ERROR, e);
        }
    }

    public static class LazyParsedViewTableMacro extends ViewTableMacro {
        private static boolean modifiable = false;
        // SqlValidatorImpl.validateSelect may call apply() many times
        // cache the parsed result here
        private CalcitePrepare.AnalyzeViewResult parsed;
        private final Supplier<CalcitePrepare.AnalyzeViewResult> parseFunction;

        public LazyParsedViewTableMacro(Supplier<CalcitePrepare.AnalyzeViewResult> parseFunction, String viewSql,
                List<String> schemaPath, List<String> viewPath) {
            super(null, viewSql, schemaPath, viewPath, modifiable);
            this.parseFunction = parseFunction;
        }

        @Override
        public TranslatableTable apply(List<Object> arguments) {
            return this.viewTable(getParsed(), viewSql, schemaPath, viewPath);
        }

        private CalcitePrepare.AnalyzeViewResult getParsed() {
            if (parsed == null) {
                parsed = parseFunction.get();
            }
            return parsed;
        }
    }

}
