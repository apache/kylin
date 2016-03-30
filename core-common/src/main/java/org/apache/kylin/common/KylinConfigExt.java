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

import java.util.Map;
import java.util.Properties;

/**
 * Extends a KylinConfig with additional overrides.
 */
@SuppressWarnings("serial")
public class KylinConfigExt extends KylinConfig {

    final private Map<String, String> overrides;

    public KylinConfigExt(KylinConfig base, Map<String, String> overrides) {
        super(base.getAllProperties());
        this.overrides = overrides;
    }
    
    protected String getOptional(String prop, String dft) {
        String value = overrides.get(prop);
        if (value != null)
            return value;
        else
            return super.getOptional(prop, dft);
    }

    protected Properties getAllProperties() {
        Properties result = new Properties(super.getAllProperties());
        result.putAll(overrides);
        return result;
    }
    
}
