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

package org.apache.kylin.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.kylin.common.util.CompositeMapView;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

/**
 * Extends a KylinConfig with additional overrides.
 */
@SuppressWarnings("serial")
public class KylinConfigExt extends KylinConfig {

    private final Map<String, String> overrides;
    final KylinConfig base;
    private final transient StrSubstitutor strSubstitutor;

    public static KylinConfigExt createInstance(KylinConfig kylinConfig, Map<String, String> overrides) {
        if (kylinConfig instanceof KylinConfigExt) {
            return new KylinConfigExt((KylinConfigExt) kylinConfig, overrides);
        } else {
            return new KylinConfigExt(kylinConfig, overrides);
        }
    }

    private KylinConfigExt(KylinConfig base, Map<String, String> overrides) {
        super(base.getRawAllProperties(), true);
        if (base.getClass() != KylinConfig.class) {
            throw new IllegalArgumentException();
        }
        this.base = base;
        this.overrides = BCC.check(overrides);
        // overrides > env > properties
        this.strSubstitutor = new StrSubstitutor(
                new CompositeMapView(this.properties, STATIC_SYSTEM_ENV, this.overrides));
    }

    private KylinConfigExt(KylinConfigExt ext, Map<String, String> overrides) {
        this(ext.base, overrides);
    }

    @Override
    public String getOptional(String prop, String dft) {
        String value = overrides.get(prop);
        if (value != null) {
            return getSubstitutor().replace(value);
        } else {
            return super.getOptional(prop, dft);
        }
    }

    @Override
    protected Properties getAllProperties() {
        Properties result = new Properties();
        result.putAll(super.getAllProperties());
        result.putAll(overrides);
        return result;
    }

    @Override
    public Map<String, String> getReadonlyProperties() {
        HashMap<String, String> config = Maps.newHashMap();
        for (Map.Entry<String, String> entry : this.overrides.entrySet()) {
            config.put(entry.getKey(), strSubstitutor.replace(entry.getValue()));
        }
        return config;
    }

    @Override
    protected StrSubstitutor getSubstitutor() {
        return this.strSubstitutor;
    }

    public Map<String, String> getExtendedOverrides() {
        return overrides;
    }

    @Override
    public KylinConfig base() {
        return this.base;
    }

}
