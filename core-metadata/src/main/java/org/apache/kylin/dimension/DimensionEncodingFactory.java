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

import com.google.common.collect.Maps;

public abstract class DimensionEncodingFactory {

    private static Map<String, DimensionEncodingFactory> factoryMap;

    /** Create a DimensionEncoding instance, with inputs corresponding to RowKeyColDesc.encodingName and RowKeyColDesc.encodingArgs. */
    public static DimensionEncoding create(String encodingName, String[] args) {
        if (factoryMap == null) {
            initFactoryMap();
        }

        DimensionEncodingFactory factory = factoryMap.get(encodingName);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown dimension encoding name " + encodingName //
                    + " (note '" + DictionaryDimEnc.ENCODING_NAME + "' is not handled by factory)");
        }

        return factory.createDimensionEncoding(encodingName, args);
    }

    public static boolean isVaildEncoding(String encodingName) {
        if (factoryMap == null) {
            initFactoryMap();
        }

        // note dictionary is a special case
        return DictionaryDimEnc.ENCODING_NAME.equals(encodingName) || factoryMap.containsKey(encodingName);
    }

    private synchronized static void initFactoryMap() {
        if (factoryMap == null) {
            Map<String, DimensionEncodingFactory> map = Maps.newConcurrentMap();

            // built-in encodings, note dictionary is a special case
            map.put(FixedLenDimEnc.ENCODING_NAME, FixedLenDimEnc.getFactory());

            // custom encodings
            String[] clsNames = KylinConfig.getInstanceFromEnv().getCubeDimensionCustomEncodingFactories();
            for (String clsName : clsNames) {
                DimensionEncodingFactory factory = (DimensionEncodingFactory) ClassUtil.newInstance(clsName);
                map.put(factory.getSupportedEncodingName(), factory);
            }
            
            factoryMap = map;
        }
    }

    /** Return the supported encoding name, corresponds to RowKeyColDesc.encodingName */
    abstract public String getSupportedEncodingName();

    /** Create a DimensionEncoding instance, with inputs corresponding to RowKeyColDesc.encodingName and RowKeyColDesc.encodingArgs */
    abstract public DimensionEncoding createDimensionEncoding(String encodingName, String[] args);
}
