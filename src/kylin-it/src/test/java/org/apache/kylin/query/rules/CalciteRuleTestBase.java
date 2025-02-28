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

package org.apache.kylin.query.rules;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.test.DiffRepository;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.QueryOptimizer;
import org.apache.kylin.query.util.HepUtils;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.util.ExecAndComp;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalciteRuleTestBase extends NLocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CalciteRuleTestBase.class);

    private final String IT_SQL_KYLIN_DIR = "../kylin-it/src/test/resources/";
    private final String emptyLinePattern = "(?m)^[ \t]*\r?\n";
    private final String NL = System.getProperty("line.separator");

    public class StringOutput {
        boolean separate;

        StringBuilder builderBefore = new StringBuilder();
        StringBuilder builderAfter = new StringBuilder();

        public StringOutput(boolean separate) {
            this.separate = separate;
        }

        public void dump(Logger log) {
            log.debug("planBefore: {}{}", NL, builderBefore.toString());
            if (separate)
                log.debug("planAfter: {}{}", NL, builderAfter.toString());
        }

        private void output(StringBuilder builder, String name, String plan) {
            builder.append("        <Resource name=\"").append(name).append("\">").append(NL);
            builder.append("            <![CDATA[");
            builder.append(plan);
            builder.append("]]>").append(NL);
            builder.append("        </Resource>").append(NL);
        }

        public void output(RelNode relBefore, RelNode relAfter, String prefix) {
            String before = Strings.isNullOrEmpty(prefix) ? "planBefore" : prefix + ".planBefore";
            final String planBefore = NL + RelOptUtil.toString(relBefore);
            output(builderBefore, separate ? prefix : before, planBefore);

            String after = Strings.isNullOrEmpty(prefix) ? "planAfter" : prefix + ".planAfter";
            final String planAfter = NL + RelOptUtil.toString(relAfter);
            output(separate ? builderAfter : builderBefore, separate ? prefix : after, planAfter);
        }
    }

    protected DiffRepository getDiffRepo() {
        throw new NotImplementedException("getDiffRepo");
    }

    protected Pair<String, String> readOneSQL(KylinConfig config, String project, String folder, String file)
            throws IOException {
        final String queryFolder = IT_SQL_KYLIN_DIR + folder;
        List<Pair<String, String>> queries = ExecAndComp.fetchQueries(queryFolder).stream().filter(e -> {
            if (Strings.isNullOrEmpty(file))
                return true;
            else
                return e.getFirst().contains(file);
        }).map(e -> {
            QueryParams queryParams = new QueryParams(config, e.getSecond(), project, 0, 0, "DEFAULT", false);
            String sql = QueryUtil.massageSql(queryParams).replaceAll(emptyLinePattern, ""); // remove empty line
            return new Pair<>(FilenameUtils.getBaseName(e.getFirst()), sql);
        }).collect(Collectors.toList());
        Assert.assertEquals(1, queries.size());
        return queries.get(0);
    }

    protected List<Pair<String, String>> readALLSQLs(KylinConfig config, String project, String folder)
            throws IOException {
        final String queryFolder = IT_SQL_KYLIN_DIR + folder;
        return ExecAndComp.fetchQueries(queryFolder).stream().map(e -> {
            QueryParams queryParams = new QueryParams(config, e.getSecond(), project, 0, 0, "DEFAULT", false);
            String sql = QueryUtil.massageSql(queryParams).replaceAll(emptyLinePattern, ""); // remove empty line
            return new Pair<>(FilenameUtils.getBaseName(e.getFirst()), sql);
        }).collect(Collectors.toList());
    }

    protected void checkDiff(RelNode relBefore, RelNode relAfter, String prefix) {
        String before = Strings.isNullOrEmpty(prefix) ? "planBefore" : prefix + ".planBefore";
        String beforeExpected = "${" + before + "}";
        final String planBefore = NL + RelOptUtil.toString(relBefore);
        getDiffRepo().assertEquals(before, beforeExpected, planBefore);

        String after = Strings.isNullOrEmpty(prefix) ? "planAfter" : prefix + ".planAfter";
        String afterExpected = "${" + after + "}";
        final String planAfter = NL + RelOptUtil.toString(relAfter);
        getDiffRepo().assertEquals(after, afterExpected, planAfter);
    }

    protected void checkSQLOptimize(String project, String sql, String prefix) {
        RelNode relBefore = sqlToRelRoot(project, sql, KylinConfig.getInstanceFromEnv()).rel;
        RelNode relAfter = toCalcitePlan(project, sql, KylinConfig.getInstanceFromEnv());
        logger.debug("check plan for {}.sql: {}{}", prefix, NL, sql);
        checkDiff(relBefore, relAfter, prefix);
    }

    protected void checkSQLPostOptimize(String project, String sql, String prefix, StringOutput StrOut,
            Collection<RelOptRule> rules) {
        RelNode relBefore = toCalcitePlan(project, sql, KylinConfig.getInstanceFromEnv());
        Assert.assertThat(relBefore, notNullValue());
        RelNode relAfter = HepUtils.runRuleCollection(relBefore, rules);
        Assert.assertThat(relAfter, notNullValue());
        logger.debug("check plan for {}.sql: {}{}", prefix, NL, sql);

        if (StrOut != null) {
            StrOut.output(relBefore, relAfter, prefix);
        } else {
            checkDiff(relBefore, relAfter, prefix);
        }
    }

    protected void checkPlanning(RelNode relBefore, RelNode relAfter, String prefix) {
        checkPlanning(relBefore, relAfter, prefix, false);
    }

    protected void checkPlanning(RelNode relBefore, RelNode relAfter, String prefix, boolean unchanged) {
        Assert.assertThat(relBefore, notNullValue());
        Assert.assertThat(relAfter, notNullValue());
        final String planBefore = NL + RelOptUtil.toString(relBefore);
        final String planAfter = NL + RelOptUtil.toString(relAfter);

        if (unchanged) {
            Assert.assertThat(planAfter, is(planBefore));
        } else {
            checkDiff(relBefore, relAfter, prefix);
        }
    }

    protected RelNode toCalcitePlan(String project, String SQL, KylinConfig kylinConfig) {
        QueryExec qe = new QueryExec(project, kylinConfig);
        try {
            return qe.parseAndOptimize(SQL);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("sql parse error", e);
        }
    }

    protected RelNode toCalcitePlanWithoutOptimize(String project, String SQL, KylinConfig kylinConfig) {
        return sqlToRelRoot(project, SQL, kylinConfig).rel;
    }

    protected RelRoot sqlToRelRoot(String project, String SQL, KylinConfig kylinConfig) {
        QueryExec qe = new QueryExec(project, kylinConfig);
        try {
            return qe.sqlToRelRoot(SQL);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("sql parse error", e);
        }
    }

    protected RelNode toCalcitePlan(String project, String SQL, KylinConfig kylinConfig,
            List<RelOptRule> rulesToRemoved, List<RelOptRule> rulesToAdded) {
        QueryExec qe = new QueryExec(project, kylinConfig);
        if (rulesToRemoved != null && !rulesToRemoved.isEmpty()) {
            qe.plannerRemoveRules(rulesToRemoved);
        }
        if (rulesToAdded != null && !rulesToAdded.isEmpty()) {
            qe.plannerAddRules(rulesToAdded);
        }
        try {
            return qe.parseAndOptimize(SQL);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("sql parse error", e);
        }
    }

    protected RelNode optimizeSQL(RelRoot relRoot, RelOptRule rule) {
        final HepProgram hepProgram = HepProgram.builder() //
                .addRuleInstance(rule) //
                .build();
        final HepPlanner planner = new HepPlanner(hepProgram);
        List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(DefaultRelMetadataProvider.INSTANCE);
        planner.registerMetadataProviders(list);
        planner.setRoot(relRoot.rel);
        return planner.findBestExp();
    }

    protected RelNode optimizeSQL(RelRoot relRoot, List<RelOptRule> rule) {
        final HepProgram hepProgram = HepProgram.builder() //
                .addRuleCollection(rule) //
                .build();
        return findBestExp(hepProgram, relRoot);
    }

    protected RelNode findBestExp(HepProgram hepProgram, RelRoot relRoot) {
        final HepPlanner planner = new HepPlanner(hepProgram);
        final QueryOptimizer optimizer = new QueryOptimizer(planner);
        RelRoot newRoot = optimizer.optimize(relRoot);
        List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(DefaultRelMetadataProvider.INSTANCE);
        planner.registerMetadataProviders(list);
        planner.setRoot(newRoot.rel);
        return planner.findBestExp();
    }
}
