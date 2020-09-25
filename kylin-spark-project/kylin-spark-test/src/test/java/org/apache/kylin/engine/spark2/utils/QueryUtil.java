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

package org.apache.kylin.engine.spark2.utils;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.apache.kylin.query.util.QueryUtil.IQueryTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class QueryUtil {
    protected static final Logger logger = LoggerFactory.getLogger(QueryUtil.class);

    static List<IQueryTransformer> queryTransformers = Collections.emptyList();
    static List<IPushDownConverter> pushDownConverters = Collections.emptyList();


    public static String massageSql(String sql, String project, int limit, int offset, String defaultSchema) {
        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        KylinConfig kylinConfig = projectInstance.getConfig();
        return massageSql(kylinConfig, sql, project, limit, offset, defaultSchema);
    }

    static String massageSql(KylinConfig kylinConfig, String sql, String project, int limit, int offset,
                             String defaultSchema) {
        String massagedSql = normalMassageSql(kylinConfig, sql, limit, offset);
        massagedSql = transformSql(kylinConfig, massagedSql, project, defaultSchema);
        logger.trace("SQL massage result: {}", massagedSql);
        return massagedSql;
    }

    private static String transformSql(KylinConfig kylinConfig, String sql, String project, String defaultSchema) {
        // customizable SQL transformation
        initQueryTransformersIfNeeded(kylinConfig);
        for (IQueryTransformer t : queryTransformers) {
            sql = t.transform(sql, project, defaultSchema);
            logger.trace("SQL transformed by {}, result: {}", t.getClass(), sql);
        }
        return sql;
    }

    static void initQueryTransformersIfNeeded(KylinConfig kylinConfig) {
        String[] currentTransformers = queryTransformers.stream().map(Object::getClass).map(Class::getCanonicalName)
                .toArray(String[]::new);
        String[] configTransformers = kylinConfig.getQueryTransformers();
        boolean transformersEqual = Objects.deepEquals(currentTransformers, configTransformers);
        if (transformersEqual) {
            return;
        }

        List<IQueryTransformer> transformers = Lists.newArrayList();
        for (String clz : configTransformers) {
            try {
                IQueryTransformer t = (IQueryTransformer) ClassUtil.newInstance(clz);
                transformers.add(t);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init query transformer", e);
            }
        }

        queryTransformers = Collections.unmodifiableList(transformers);
    }

    public static String massagePushDownSql(String sql, String project, String defaultSchema, boolean isPrepare) {
        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        KylinConfig kylinConfig = projectInstance.getConfig();
        while (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1);

        return massagePushDownSql(kylinConfig, sql, project, defaultSchema, isPrepare);
    }

    static String massagePushDownSql(KylinConfig kylinConfig, String sql, String project, String defaultSchema,
                                     boolean isPrepare) {
        initPushDownConvertersIfNeeded(kylinConfig);
        for (IPushDownConverter converter : pushDownConverters) {
            sql = converter.convert(sql, project, defaultSchema, isPrepare);
        }
        return sql;
    }

    static void initPushDownConvertersIfNeeded(KylinConfig kylinConfig) {
        String[] currentConverters = pushDownConverters.stream().map(Object::getClass).map(Class::getCanonicalName)
                .toArray(String[]::new);
        String[] configConverters = kylinConfig.getPushDownConverterClassNames();
        boolean skipInit = Objects.deepEquals(currentConverters, configConverters);

        if (skipInit) {
            return;
        }

        List<IPushDownConverter> converters = Lists.newArrayList();
        for (String clz : configConverters) {
            try {
                IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(clz);
                converters.add(converter);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init pushdown converter", e);
            }
        }
        pushDownConverters = Collections.unmodifiableList(converters);
    }

    public static String normalMassageSql(KylinConfig kylinConfig, String sql, int limit, int offset) {
        sql = sql.trim();
        sql = sql.replace("\r", " ").replace("\n", System.getProperty("line.separator"));

        while (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1);

        if (limit > 0 && !sql.toLowerCase(Locale.ROOT).contains("limit")) {
            sql += ("\nLIMIT " + limit);
        }

        if (offset > 0 && !sql.toLowerCase(Locale.ROOT).contains("offset")) {
            sql += ("\nOFFSET " + offset);
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        if (kylinConfig.getForceLimit() > 0 && !sql.toLowerCase(Locale.ROOT).contains("limit")
                && sql.toLowerCase(Locale.ROOT).matches("^select\\s+\\*\\p{all}*")) {
            sql += ("\nLIMIT " + kylinConfig.getForceLimit());
        }
        return sql;
    }
}
