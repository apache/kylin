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

package org.apache.kylin.storage.druid;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

public class NameMappingFactory {

    public static NameMapping getDefault(CubeDesc desc) {
        boolean noDupName = true;
        Set<String> names = Sets.newHashSet();

        for (RowKeyColDesc col : desc.getRowkey().getRowKeyColumns()) {
            noDupName = noDupName && names.add(col.getColRef().getName());
        }
        for (MeasureDesc met : desc.getMeasures()) {
            noDupName = noDupName && names.add(met.getName());
        }

        return noDupName ? DirectNameMapping.INSTANCE : new CachedNameMapping();
    }

    private static class DirectNameMapping implements NameMapping {
        static DirectNameMapping INSTANCE = new DirectNameMapping();

        @Override
        public String getDimFieldName(TblColRef dim) {
            return dim.getName();
        }

        @Override
        public String getMeasureFieldName(MeasureDesc met) {
            return met.getName();
        }
    }

    private static class CachedNameMapping implements NameMapping {
        static final String DIM_PREFIX = "D_";
        static final String MEASURE_PREFIX = "M_";

        final Map<TblColRef, String> dimNameCache = new HashMap<>();
        final Map<String, String> metNameCache = new HashMap<>();

        @Override
        public String getDimFieldName(TblColRef dim) {
            String name = dimNameCache.get(dim);
            if (name == null) {
                // prefer "@" to "." in column name
                name = DIM_PREFIX + dim.getTableAlias() + "@" + dim.getName();
                dimNameCache.put(dim, name);
            }
            return name;
        }

        @Override
        public String getMeasureFieldName(MeasureDesc met) {
            String name = metNameCache.get(met.getName());
            if (name == null) {
                name = MEASURE_PREFIX + met.getName();
                metNameCache.put(met.getName(), name);
            }
            return name;
        }
    }
}
