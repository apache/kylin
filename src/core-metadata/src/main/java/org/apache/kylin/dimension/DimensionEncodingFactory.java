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

package org.apache.kylin.dimension;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public abstract class DimensionEncodingFactory {

    private static final Logger logger = LoggerFactory.getLogger(DimensionEncodingFactory.class);

    private static Map<Pair<String, Integer>, DimensionEncodingFactory> factoryMap;

    /**
     * If a bug found in a DimEnc will cause different cube outputs,
     * we'll have to increase the version number of DimEnc, in order
     * to distinguish current version with prior version.
     * <p>
     * The default version applys to all existing legacy DimEncs
     */
    protected int getCurrentVersion() {
        return 1;
    }

    /**
     * Create a DimensionEncoding instance, with inputs corresponding to RowKeyColDesc.encodingName and RowKeyColDesc.encodingArgs.
     */
    public static DimensionEncoding create(String encodingName, String[] args, int version) {
        if (factoryMap == null)
            initFactoryMap();

        DimensionEncodingFactory factory = factoryMap.get(Pair.newPair(encodingName, version));
        if (factory == null) {
            throw new IllegalArgumentException("Unknown dimension encoding name " + encodingName //
                    + " (note '" + DictionaryDimEnc.ENCODING_NAME + "' is not handled by factory)");
        }

        return factory.createDimensionEncoding(encodingName, args);
    }

    public static Map<String, Integer> getValidEncodings() {
        if (factoryMap == null)
            initFactoryMap();

        Map<String, Integer> result = Maps.newTreeMap();
        for (Pair<String, Integer> p : factoryMap.keySet()) {
            if (result.containsKey(p.getFirst())) {
                if (result.get(p.getFirst()) > p.getSecond()) {
                    continue;//skip small versions
                }
            }

            result.put(p.getFirst(), p.getSecond());
        }
        result.put(DictionaryDimEnc.ENCODING_NAME, 1);
        return result;
    }

    public static boolean isValidEncoding(final String encodingName) {
        if (factoryMap == null)
            initFactoryMap();

        // note dictionary is a special case
        return DictionaryDimEnc.ENCODING_NAME.equals(encodingName) || //
                Iterables.any(factoryMap.keySet(), input -> input != null && input.getFirst().equals(encodingName));
    }

    private synchronized static void initFactoryMap() {
        if (factoryMap == null) {
            Map<Pair<String, Integer>, DimensionEncodingFactory> map = Maps.newConcurrentMap();

            // built-in encodings, note dictionary is a special case
            {
                FixedLenDimEnc.Factory value = new FixedLenDimEnc.Factory();
                map.put(Pair.newPair(FixedLenDimEnc.ENCODING_NAME, value.getCurrentVersion()), value);
            }
            {
                IntDimEnc.Factory value = new IntDimEnc.Factory();
                map.put(Pair.newPair(IntDimEnc.ENCODING_NAME, value.getCurrentVersion()), value);
            }
            {
                IntegerDimEnc.Factory value = new IntegerDimEnc.Factory();
                map.put(Pair.newPair(IntegerDimEnc.ENCODING_NAME, value.getCurrentVersion()), value);
            }
            {
                FixedLenHexDimEnc.Factory value = new FixedLenHexDimEnc.Factory();
                map.put(Pair.newPair(FixedLenHexDimEnc.ENCODING_NAME, value.getCurrentVersion()), value);
            }
            {
                DateDimEnc.Factory value = new DateDimEnc.Factory();
                map.put(Pair.newPair(DateDimEnc.ENCODING_NAME, value.getCurrentVersion()), value);
            }
            {
                TimeDimEnc.Factory value = new TimeDimEnc.Factory();
                map.put(Pair.newPair(TimeDimEnc.ENCODING_NAME, value.getCurrentVersion()), value);
            }
            {
                BooleanDimEnc.Factory value = new BooleanDimEnc.Factory();
                map.put(Pair.newPair(BooleanDimEnc.ENCODING_NAME, value.getCurrentVersion()), value);
            }

            // custom encodings
            String[] clsNames = KylinConfig.getInstanceFromEnv().getCubeDimensionCustomEncodingFactories();
            for (String clsName : clsNames) {
                try {
                    DimensionEncodingFactory factory = (DimensionEncodingFactory) ClassUtil.newInstance(clsName);
                    map.put(Pair.newPair(factory.getSupportedEncodingName(), factory.getCurrentVersion()), factory);
                } catch (Exception ex) {
                    logger.error("Failed to init dimension encoding factory " + clsName, ex);
                }
            }

            factoryMap = map;
        }
    }

    /**
     * Return the supported encoding name, corresponds to RowKeyColDesc.encodingName
     */
    abstract public String getSupportedEncodingName();

    /**
     * Create a DimensionEncoding instance, with inputs corresponding to RowKeyColDesc.encodingName and RowKeyColDesc.encodingArgs
     */
    abstract public DimensionEncoding createDimensionEncoding(String encodingName, String[] args);
}
