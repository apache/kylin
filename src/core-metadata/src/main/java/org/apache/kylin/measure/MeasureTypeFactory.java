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

package org.apache.kylin.measure;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigCannotInitException;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.measure.bitmap.intersect.IntersectMeasureType;
import org.apache.kylin.measure.collect_set.CollectSetMeasureType;
import org.apache.kylin.measure.corr.CorrMeasureType;
import org.apache.kylin.measure.dim.DimCountDistinctMeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.measure.hllc.HLLCMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.measure.raw.RawMeasureType;
import org.apache.kylin.measure.sumlc.SumLCMeasureType;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

/**
 * Factory for MeasureType.
 * 
 * The factory registers itself by claiming the aggregation function and data type it supports,
 * to match a measure descriptor in cube definition.
 * 
 * E.g. HyperLogLog measure type claims "COUNT_DISCINT" as function and "hllc" as data type to
 * match measure descriptor:
 * <pre>
  {
    "name" : "SELLER_CNT_HLL",
    "function" : {
      "expression" : "COUNT_DISTINCT",        <----  function name
      "parameter" : {
        "type" : "column",
        "value" : "SELLER_ID",
        "next_parameter" : null
      },
      "returntype" : "hllc(10)"               <----  data type
    }
  }
</pre>
 * 
 * @param <T> the Java type of aggregation data object, e.g. HLLCounter
 */
// TODO remove this over complicated factory
abstract public class MeasureTypeFactory<T> {

    private static final Logger logger = LoggerFactory.getLogger(MeasureTypeFactory.class);

    /**
     * Create a measure type with specified aggregation function and data type.
     * 
     * @param funcName should always match this factory's claim <code>getAggrFunctionName()</code>
     * @param dataType should always match this factory's claim <code>getAggrDataTypeName()</code>
     */
    abstract public MeasureType<T> createMeasureType(String funcName, DataType dataType);

    /** Return the aggregation function this factory supports, like "COUNT_DISTINCT" */
    abstract public String getAggrFunctionName();

    /** Return the aggregation data type name this factory supports, like "hllc" */
    abstract public String getAggrDataTypeName();

    /** Return the Serializer for aggregation data object. Note a Serializer implementation must be thread-safe! */
    abstract public Class<? extends DataTypeSerializer<T>> getAggrDataTypeSerializer();

    // ============================================================================

    final private static Map<String, List<MeasureTypeFactory<?>>> factories = Maps.newHashMap();
    final private static Map<String, Class<?>> udafMap = Maps.newHashMap(); // a map from UDAF to Calcite aggregation function implementation class
    final private static Map<String, MeasureTypeFactory> udafFactories = Maps.newHashMap(); // a map from UDAF to its owner factory
    final private static List<MeasureTypeFactory<?>> defaultFactory = Lists.newArrayListWithCapacity(2);

    static {
        init();
    }

    public static synchronized void init() {
        if (!factories.isEmpty()) {
            return;
        }

        List<MeasureTypeFactory<?>> factoryInsts = Lists.newArrayList();

        // built-in advanced measure types
        factoryInsts.add(new HLLCMeasureType.Factory());
        factoryInsts.add(new BitmapMeasureType.Factory());
        factoryInsts.add(new TopNMeasureType.Factory());
        factoryInsts.add(new RawMeasureType.Factory());
        factoryInsts.add(new ExtendedColumnMeasureType.Factory());
        factoryInsts.add(new PercentileMeasureType.Factory());
        factoryInsts.add(new DimCountDistinctMeasureType.Factory());
        factoryInsts.add(new IntersectMeasureType.Factory());
        factoryInsts.add(new CollectSetMeasureType.Factory());
        factoryInsts.add(new CorrMeasureType.Factory());
        factoryInsts.add(new SumLCMeasureType.Factory());

        logger.info("Checking custom measure types from kylin config");

        try {
            Map<String, String> customMeasureTypes = KylinConfig.getInstanceFromEnv().getCubeCustomMeasureTypes();
            for (String customFactory : customMeasureTypes.values()) {
                try {
                    logger.info("Checking custom measure types from kylin config: " + customFactory);
                    factoryInsts.add((MeasureTypeFactory<?>) Class.forName(customFactory).newInstance());
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unrecognized MeasureTypeFactory classname: " + customFactory,
                            e);
                }
            }
        } catch (KylinConfigCannotInitException e) {
            logger.warn("Will not add custome MeasureTypeFactory as KYLIN_CONF nor KYLIN_HOME is set");
        }

        // register factories & data type serializers
        for (MeasureTypeFactory<?> factory : factoryInsts) {
            String funcName = factory.getAggrFunctionName();
            if (!funcName.equals(funcName.toUpperCase(Locale.ROOT)))
                throw new IllegalArgumentException(
                        "Aggregation function name '" + funcName + "' must be in upper case");
            String dataTypeName = factory.getAggrDataTypeName();
            if (!dataTypeName.equals(dataTypeName.toLowerCase(Locale.ROOT)))
                throw new IllegalArgumentException(
                        "Aggregation data type name '" + dataTypeName + "' must be in lower case");
            Class<? extends DataTypeSerializer<?>> serializer = factory.getAggrDataTypeSerializer();

            logger.info("registering " + funcName + "(" + dataTypeName + "), " + factory.getClass());
            DataType.register(dataTypeName);
            DataTypeSerializer.register(dataTypeName, serializer);
            registerUDAF(factory);
            List<MeasureTypeFactory<?>> list = factories.get(funcName);
            if (list == null)
                list = Lists.newArrayListWithCapacity(2);
            factories.put(funcName, list);
            list.add(factory);
        }

        defaultFactory.add(new BasicMeasureType.Factory());
    }

    private static void registerUDAF(MeasureTypeFactory<?> factory) {
        MeasureType<?> type = factory.createMeasureType(factory.getAggrFunctionName(),
                DataType.getType(factory.getAggrDataTypeName()));
        Map<String, Class<?>> udafs = type.getRewriteCalciteAggrFunctions();
        if (!type.needRewrite() || udafs == null)
            return;

        for (String udaf : udafs.keySet()) {
            udaf = udaf.toUpperCase(Locale.ROOT);
            if (udaf.equals(FunctionDesc.FUNC_COUNT_DISTINCT))
                continue; // skip built-in function

            if (udafFactories.containsKey(udaf))
                throw new IllegalStateException(
                        "UDAF '" + udaf + "' was dup declared by " + udafFactories.get(udaf) + " and " + factory);

            udafFactories.put(udaf, factory);
            udafMap.put(udaf, udafs.get(udaf));
        }
    }

    public static Map<String, Class<?>> getUDAFs() {
        return udafMap;
    }

    public static Map<String, MeasureTypeFactory> getUDAFFactories() {
        return udafFactories;
    }

    public static MeasureType<?> create(String funcName, String dataType) {
        return create(funcName, DataType.getType(dataType));
    }

    public static MeasureType<?> createNoRewriteFieldsMeasureType(String funcName, DataType dataType) {
        // currently only has DimCountDistinctAgg
        if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_COUNT_DISTINCT)) {
            return new DimCountDistinctMeasureType.Factory().createMeasureType(funcName, dataType);
        }

        throw new UnsupportedOperationException("No measure type found.");
    }

    public static MeasureType<?> create(String funcName, DataType dataType) {
        funcName = funcName.toUpperCase(Locale.ROOT);

        List<MeasureTypeFactory<?>> factory = factories.get(funcName);
        if (factory == null)
            factory = defaultFactory;

        // a special case where in early stage of sql parsing, the data type is unknown; only needRewrite() is required at that stage
        if (dataType == null) {
            return new NeedRewriteOnlyMeasureType(funcName, factory);
        }

        // the normal case, only one factory for a function
        if (factory.size() == 1) {
            return factory.get(0).createMeasureType(funcName, dataType);
        }

        // sometimes multiple factories are registered for the same function, then data types must tell them apart
        for (MeasureTypeFactory<?> f : factory) {
            if (f.getAggrDataTypeName().equals(dataType.getName()))
                return f.createMeasureType(funcName, dataType);
        }
        throw new IllegalStateException(
                "failed to create MeasureType with funcName: " + funcName + ", dataType: " + dataType);
    }

    @SuppressWarnings("rawtypes")
    public static class NeedRewriteOnlyMeasureType extends MeasureType {

        private Boolean needRewrite;

        public NeedRewriteOnlyMeasureType(String funcName, List<MeasureTypeFactory<?>> factory) {
            for (MeasureTypeFactory<?> f : factory) {
                boolean b = f.createMeasureType(funcName, null).needRewrite();
                if (needRewrite == null)
                    needRewrite = Boolean.valueOf(b);
                else if (needRewrite.booleanValue() != b)
                    throw new IllegalStateException(
                            "needRewrite() of factorys " + factory + " does not have consensus");
            }
        }

        @Override
        public MeasureIngester newIngester() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MeasureAggregator newAggregator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean needRewrite() {
            return needRewrite;
        }

        @Override
        public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
            return null;
        }

    }
}
