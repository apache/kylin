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

package org.apache.kylin.rec.common;

import java.io.Serializable;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;

import lombok.Getter;

public class SmartConfig implements Serializable {
    @Getter
    private final KapConfig kapConfig;

    private SmartConfig(KapConfig kapConfig) {
        this.kapConfig = kapConfig;
    }

    public static SmartConfig wrap(KylinConfig kylinConfig) {
        return new SmartConfig(KapConfig.wrap(kylinConfig));
    }

    public KylinConfig getKylinConfig() {
        return kapConfig.getKylinConfig();
    }

    private String getOptional(String name, String defaultValue) {
        String val = kapConfig.getSmartModelingConf(name);
        if (val == null) {
            return defaultValue;
        } else {
            return val.trim();
        }
    }

    private long getOptional(String name, long defaultValue) {
        return Long.parseLong(getOptional(name, Long.toString(defaultValue)));
    }

    private boolean getOptional(String name, boolean defaultValue) {
        return Boolean.parseBoolean(getOptional(name, Boolean.toString(defaultValue)));
    }

    public long getRowkeyUHCCardinalityMin() {
        return getOptional("rowkey.uhc.min-cardinality", 1000000L);
    }

    public String getMeasureCountDistinctType() {
        return getOptional("measure.count-distinct.return-type", FunctionDesc.FUNC_COUNT_DISTINCT_BIT_MAP);
    }

    public long getComputedColumnOnGroupKeySuggestionMinCardinality() {
        return getOptional("computed-column.suggestion.group-key.minimum-cardinality", 10000L);
    }

    public long getComputedColumnOnFilterKeySuggestionMinCardinality() {
        return getOptional("computed-column.suggestion.filter-key.minimum-cardinality", 10000L);
    }

    public boolean enableComputedColumnOnFilterKeySuggestion() {
        return Boolean.parseBoolean(getOptional("computed-column.suggestion.filter-key.enabled", "FALSE"));
    }

    public boolean needProposeCcIfNoSampling() {
        return getOptional("computed-column.suggestion.enabled-if-no-sampling", false);
    }

    public boolean includeAllFks() {
        return getOptional("propose-all-foreign-keys", true);
    }

    public String getModelOptRule() {
        return getOptional("model-opt-rule", "");
    }

    public boolean skipUselessMetadata() {
        return getOptional("skip-useless-metadata", true);
    }

    public String getProposeRunnerImpl() {
        return getOptional("propose-runner-type", "fork");
    }
}
