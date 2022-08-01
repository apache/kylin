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

import java.util.Locale;
import java.util.Properties;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.kylin.common.KylinConfig;

/**
 * wrapper for calcite configs defined in kylin props
 */
public class KECalciteConfig extends CalciteConnectionConfigImpl {

    static final ThreadLocal<KECalciteConfig> THREAD_LOCAL = new ThreadLocal<>();

    private KylinConfig kylinConfig;

    private KECalciteConfig(Properties properties, KylinConfig kylinConfig) {
        super(properties);
        this.kylinConfig = kylinConfig;
    }

    public static KECalciteConfig fromKapConfig(KylinConfig kylinConfig) {
        Properties props = new Properties();
        props.putAll(kylinConfig.getCalciteExtrasProperties());
        return new KECalciteConfig(props, kylinConfig);
    }

    public static KECalciteConfig current() {
        return THREAD_LOCAL.get();
    }

    @Override
    public NullCollation defaultNullCollation() {
        return NullCollation.LOW;
    }

    @Override
    public boolean caseSensitive() {
        return false;
    }

    @Override
    public Casing unquotedCasing() {
        return this.kylinConfig.getSourceNameCaseSensitiveEnabled() ? Casing.UNCHANGED : super.unquotedCasing();
    }

    public boolean exposeComputedColumn() {
        return kylinConfig.exposeComputedColumn();
    }

    public int getIdentifierMaxLength() {
        return kylinConfig.getMaxModelDimensionMeasureNameLength();
    }

    public String[] operatorTables() {
        return CalciteConnectionProperty.FUN.wrap(properties).getString("standard").toLowerCase(Locale.ROOT).split(",");
    }
}
