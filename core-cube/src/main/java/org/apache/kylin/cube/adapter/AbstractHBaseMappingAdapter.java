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

package org.apache.kylin.cube.adapter;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public abstract class AbstractHBaseMappingAdapter implements IHBaseMappingAdapter, IWiseHBaseMapper {


    public static final AbstractHBaseMappingAdapter getHBaseAdapter(KylinConfig config) {
        try {
            Class<? extends AbstractHBaseMappingAdapter> adapterClass =
                    ClassUtil.forName(config.getAutoHBaseColumnFamilyMapper(), AbstractHBaseMappingAdapter.class);
            Constructor<? extends AbstractHBaseMappingAdapter> constructor = adapterClass.getConstructor(KylinConfig.class);
            return constructor.newInstance(config);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | IllegalAccessException
                |InstantiationException
                | InvocationTargetException e) {
            throw new IllegalArgumentException("Can't reflect HbaseMapper: " + config.getAutoHBaseColumnFamilyMapper());
        }
    }

    protected KylinConfig config;
    final protected String hBaseColumnFamilyNamePrefix;
    final protected String hBaseColumnQualifierPrefix;

    protected AbstractHBaseMappingAdapter(KylinConfig config) {
        this.config = config;
        hBaseColumnFamilyNamePrefix = this.config.getHBaseColumnFamilyNamePrefix();
        hBaseColumnQualifierPrefix = this.config.getHBaseColumnQualifierPrefix();
    }

    protected AbstractHBaseMappingAdapter() {
        this(KylinConfig.getInstanceFromEnv());
    }

    @Override
    public void initHBaseMapping(CubeDesc cubeDesc) {
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        if (null == measures) {
            throw new IllegalArgumentException("cubeDesc.getMeasures got null.");
        }
        boolean needResetHBaseMapping = config.isAutoHBaseMappingEnable() && null == cubeDesc.getHbaseMapping();
        if (needResetHBaseMapping) {
            HBaseMappingDesc hbaseMapping = getHBaseMappingOnlyOnce(cubeDesc);
            cubeDesc.setHbaseMapping(hbaseMapping);
        }
        cubeDesc.getHbaseMapping().init(cubeDesc);
    }

    @Override
    public void initMeasureReferenceToColumnFamilyWithChecking(CubeDesc cubeDesc) {
        cubeDesc.initMeasureReferenceToColumnFamily();
    }

    protected HBaseColumnFamilyDesc getSingleColumnCFContain(MeasureDesc measure, String cfName) {
        HBaseColumnFamilyDesc ret = new HBaseColumnFamilyDesc();
        HBaseColumnDesc[] hbaseColumnDescs = {getColumnContainSingleMeasure(measure, hBaseColumnQualifierPrefix)};
        ret.setColumns(hbaseColumnDescs);
        ret.setName(cfName);
        return ret;
    }

    protected HBaseColumnDesc getHBaseColumnDescWithName(String qualifier) {
        HBaseColumnDesc normColumn = new HBaseColumnDesc();
        normColumn.setQualifier(qualifier);
        return normColumn;
    }

    protected  MeasureDesc getMeasureByName(CubeDesc desc, String measureName) {
        for (MeasureDesc m : desc.getMeasures()) {
            if (m.getName().equals(measureName)) {
                return m;
            }
        }
        throw new IllegalStateException(String.format(Locale.ROOT, "Can't find measure %s in cube %s", measureName, desc.getName()));
    }


    protected MeasureType<?> getMeasureType(MeasureDesc measure) {
        FunctionDesc func = measure.getFunction();
        func.setReturnType(func.getReturnType());
        MeasureType<?> measureType = func.getMeasureType();
        if (null == measureType) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "measure type of %s can't be null!", measure.getName()));
        }
        return measureType;
    }

    // only copy reference
    protected HBaseMappingDesc copy(HBaseMappingDesc needCopy) {
        List<HBaseColumnFamilyDesc> copyCFs = Lists.newArrayListWithCapacity(needCopy.getColumnFamily().length);
        for (HBaseColumnFamilyDesc cf : needCopy.getColumnFamily()) {
            List<HBaseColumnDesc> copyColumns = Lists.newArrayListWithCapacity(cf.getColumns().length);
            for (HBaseColumnDesc c : cf.getColumns()) {
                List<String> copyMeasureRefs = Lists.newArrayListWithCapacity(c.getMeasureRefs().length);
                for (String mf : c.getMeasureRefs()) {
                    copyMeasureRefs.add(mf);
                }
                HBaseColumnDesc copyC = getHBaseColumnDescWithName(c.getQualifier());
                copyC.setMeasureRefs(copyMeasureRefs.toArray(new String[0]));
                copyColumns.add(copyC);
            }
            HBaseColumnFamilyDesc copyCF = new HBaseColumnFamilyDesc();
            copyCF.setName(cf.getName());
            copyCF.setColumns(copyColumns.toArray(new HBaseColumnDesc[0]));
            copyCFs.add(copyCF);
        }
        HBaseMappingDesc copyMapping = new HBaseMappingDesc();
        copyMapping.setColumnFamily(copyCFs.toArray(new HBaseColumnFamilyDesc[0]));
        return copyMapping;
    }

    /**
     *
     * @param cubeName
     * @return  [index, measure cnt], sorted by measure cnt asc
     */
    protected List<Pair<Integer, Integer>> getNormCFIndexInOldMapping(String cubeName){
        List<Pair<Integer, Integer>> normCFIndexes = Lists.newArrayListWithExpectedSize(5);
        HBaseColumnFamilyDesc[] cfs = getCubeDescManager().getCubeDesc(cubeName).getHbaseMapping().getColumnFamily();
        for (int i = 0; i < cfs.length; i++) {
            if (!cfs[i].isMemoryHungry()) {
                normCFIndexes.add(new Pair<>(i, getMeasureNumberInCF(cfs[i])));
            }
        }
        normCFIndexes.sort(Comparator.comparingInt(Pair::getSecond));
        return normCFIndexes;
    }

    protected int getMeasureNumberInCF(HBaseColumnFamilyDesc cf) {
        int ret = 0;
        for (HBaseColumnDesc c : cf.getColumns()) {
            ret += c.getMeasureRefs().length;
        }
        return ret;
    }

    protected HBaseColumnDesc getColumnContainSingleMeasure(MeasureDesc measure, String qualifier) {
        return getColumnContainSingleMeasure(measure.getName(), qualifier);
    }

    protected HBaseColumnDesc getColumnContainSingleMeasure(String measureName, String qualifier) {
        HBaseColumnDesc hbaseColumn = getHBaseColumnDescWithName(qualifier);
        hbaseColumn.setMeasureRefs(new String[]{measureName});
        return hbaseColumn;
    }

    protected CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(config);
    }

    protected int findMaxSuffixOfCF(List<HBaseColumnFamilyDesc> cfs) {
        if (cfs == null || cfs.size() == 0) {
            return 0;
        }
        List<Integer> cfSuffixes = cfs.stream()
                .map(cf -> NumberUtils.toInt(cf.getName().replace(hBaseColumnFamilyNamePrefix, "")))
                .sorted()
                .collect(Collectors.toList());
        return cfSuffixes.get(cfSuffixes.size() - 1);
    }

    protected int findMaxSuffixOfColumn(List<HBaseColumnDesc> cs) {
        if (cs == null || cs.size() == 0) {
            return 0;
        }
        List<Integer> cSuffixes = cs.stream()
                .map(cf -> NumberUtils.toInt(cf.getQualifier().replace(hBaseColumnQualifierPrefix, "")))
                .sorted()
                .collect(Collectors.toList());
        return cSuffixes.get(cSuffixes.size() - 1);
    }
}
