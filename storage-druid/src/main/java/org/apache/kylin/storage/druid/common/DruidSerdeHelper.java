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

package org.apache.kylin.storage.druid.common;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.druid.initialization.DruidModule;
import io.druid.jackson.DefaultObjectMapper;
//import io.druid.query.aggregation.decimal.DecimalDruidModule;
//import io.druid.query.aggregation.decimal.DecimalMaxSerde;
//import io.druid.query.aggregation.decimal.DecimalMinSerde;
//import io.druid.query.aggregation.decimal.DecimalSumSerde;
//import io.druid.query.aggregation.kylin.distinctcount.DistinctCountDruidModule;
//import io.druid.query.aggregation.kylin.distinctcount.DistinctCountSerde;
//import io.druid.query.aggregation.kylin.extendcolumn.ExtendColumnAggregatorFactory;
//import io.druid.query.aggregation.kylin.extendcolumn.ExtendColumnDruidModule;
//import io.druid.query.aggregation.kylin.extendcolumn.ExtendColumnSerde;
//import io.druid.segment.serde.ComplexMetrics;

public class DruidSerdeHelper {
    private static final DruidModule[] druidModules = {
//            new DistinctCountDruidModule(),
//            new ExtendColumnDruidModule(),
//            new DecimalDruidModule()
    };

    public static final ObjectMapper JSON_MAPPER;

    static {
        JSON_MAPPER = new DefaultObjectMapper();
        for (DruidModule module : druidModules) {
            for (Module jacksonModule : module.getJacksonModules()) {
                JSON_MAPPER.registerModule(jacksonModule);
            }
        }
        JSON_MAPPER.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
    }

    //we will support distinctCount, ExtendColumn, Decimal metrics later
//    public static void registerDruidSerde() {
//        if (ComplexMetrics.getSerdeForType("kylin-distinctCount") == null) {
//            ComplexMetrics.registerSerde("kylin-distinctCount", new DistinctCountSerde());
//        }
//
//        if (ComplexMetrics.getSerdeForType(ExtendColumnAggregatorFactory.EXTEND_COLUMN) == null) {
//            ComplexMetrics.registerSerde(ExtendColumnAggregatorFactory.EXTEND_COLUMN, new ExtendColumnSerde());
//        }
//
//        if (ComplexMetrics.getSerdeForType(DecimalDruidModule.DECIMALSUM) == null) {
//            ComplexMetrics.registerSerde(DecimalDruidModule.DECIMALSUM, new DecimalSumSerde());
//        }
//
//        if (ComplexMetrics.getSerdeForType(DecimalDruidModule.DECIMALMIN) == null) {
//            ComplexMetrics.registerSerde(DecimalDruidModule.DECIMALMIN, new DecimalMinSerde());
//        }
//
//        if (ComplexMetrics.getSerdeForType(DecimalDruidModule.DECIMALMAX) == null) {
//            ComplexMetrics.registerSerde(DecimalDruidModule.DECIMALMAX, new DecimalMaxSerde());
//        }
//    }

}
