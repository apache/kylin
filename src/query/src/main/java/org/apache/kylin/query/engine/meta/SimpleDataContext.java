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

package org.apache.kylin.query.engine.meta;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.kylin.common.KylinConfig;

/**
 * a simple data context holder for schema, typeFactory, contextVariable, dynamicVariable
 */
public class SimpleDataContext implements MutableDataContext {

    private final SchemaPlus rootSchema;
    private final JavaTypeFactory javaTypeFactory;
    private final KylinConfig kylinConfig;
    private final Map<String, Object> contextVars = new HashMap<>();
    private boolean isContentQuery = false;

    public SimpleDataContext(SchemaPlus rootSchema, JavaTypeFactory javaTypeFactory, KylinConfig kylinConfig) {
        this.rootSchema = rootSchema;
        this.javaTypeFactory = javaTypeFactory;
        this.kylinConfig = kylinConfig;
        initContextVars();
    }

    private void initContextVars() {
        putContextVar(Variable.TIME_ZONE.camelName, TimeZone.getTimeZone(kylinConfig.getTimeZone()));
    }

    @Override
    public SchemaPlus getRootSchema() {
        return rootSchema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return javaTypeFactory;
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(String name) {
        return contextVars.get(name);
    }

    @Override
    public void putContextVar(String name, Object value) {
        contextVars.put(name, value);
    }

    public void setPrepareParam(int idx, Object value) {
        contextVars.put(prepareParamName(idx), value);
    }

    private String prepareParamName(int idx) {
        return String.format(Locale.ROOT, "?%d", idx);
    }

    public KylinConfig getKylinConfig() {
        return kylinConfig;
    }

    public boolean isContentQuery() {
        return isContentQuery;
    }

    public void setContentQuery(boolean contentQuery) {
        isContentQuery = contentQuery;
    }
}
