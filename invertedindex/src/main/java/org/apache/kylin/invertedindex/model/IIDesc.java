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

package org.apache.kylin.invertedindex.model;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.*;

/**
 * @author yangli9
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class IIDesc extends RootPersistentEntity {

    public static final String HBASE_FAMILY = "f";
    public static final String HBASE_QUALIFIER = "c";
    public static final String HBASE_DICTIONARY = "d";
    public static final byte[] HBASE_FAMILY_BYTES = Bytes.toBytes(HBASE_FAMILY);
    public static final byte[] HBASE_QUALIFIER_BYTES = Bytes.toBytes(HBASE_QUALIFIER);
    public static final byte[] HBASE_DICTIONARY_BYTES = Bytes.toBytes(HBASE_DICTIONARY);

    private KylinConfig config;
    private DataModelDesc model;

    @JsonProperty("name")
    private String name;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("timestamp_dimension")
    private String timestampDimension;
    @JsonProperty("bitmap_dimensions")
    private List<DimensionDesc> bitmapDimensions = Collections.emptyList();
    @JsonProperty("value_dimensions")
    private List<DimensionDesc> valueDimensions;
    @JsonProperty("metrics")
    private String[] metricNames;
    @JsonProperty("sharding")
    private short sharding = 1; // parallelism
    @JsonProperty("slice_size")
    private int sliceSize = 50000; // no. rows
    @JsonProperty("signature")
    private String signature;

    // computed
    private List<TableDesc> allTables = Lists.newArrayList();
    private List<TblColRef> allColumns = Lists.newArrayList();
    private List<TblColRef> allDimensions = Lists.newArrayList();
    private int tsCol;
    private int[] bitmapCols;
    private int[] valueCols;
    private int[] metricsCols;
    private BitSet metricsColSet;
    private List<MeasureDesc> measureDescs;

    public void init(MetadataManager metadataManager) {

        config = metadataManager.getConfig();

        if (this.modelName == null || this.modelName.length() == 0) {
            throw new RuntimeException("The cubeDesc '" + this.getName() + "' doesn't have data model specified.");
        }

        this.model = MetadataManager.getInstance(config).getDataModelDesc(this.modelName);

        if (this.model == null) {
            throw new RuntimeException("No data model found with name '" + modelName + "'.");
        }

        timestampDimension = timestampDimension.toUpperCase();

        // capitalize
        DimensionDesc.capicalizeStrings(bitmapDimensions);
        DimensionDesc.capicalizeStrings(valueDimensions);
        StringUtil.toUpperCaseArray(metricNames, metricNames);

        // retrieve all columns and all tables, and make available measure to ii
        HashSet<String> allTableNames = Sets.newHashSet();
        measureDescs = Lists.newArrayList();
        measureDescs.add(makeCountMeasure());
        for (DimensionDesc dimensionDesc : Iterables.concat(bitmapDimensions, valueDimensions)) {
            TableDesc tableDesc = this.getTableDesc(dimensionDesc.getTable());
            for (String column : dimensionDesc.getColumns()) {
                ColumnDesc columnDesc = tableDesc.findColumnByName(column);
                TblColRef tcr = new TblColRef(columnDesc);
                allColumns.add(tcr);
                allDimensions.add(tcr);
                measureDescs.add(makeHLLMeasure(columnDesc, null));
            }

            if (!allTableNames.contains(tableDesc.getIdentity())) {
                allTableNames.add(tableDesc.getIdentity());
                allTables.add(tableDesc);
            }
        }
        for (String column : metricNames) {
            TableDesc tableDesc = this.getTableDesc(this.getFactTableName());
            ColumnDesc columnDesc = tableDesc.findColumnByName(column);
            allColumns.add(new TblColRef(columnDesc));
            measureDescs.add(makeNormalMeasure("SUM", columnDesc));
            measureDescs.add(makeNormalMeasure("MIN", columnDesc));
            measureDescs.add(makeNormalMeasure("MAX", columnDesc));
            if (!allTableNames.contains(tableDesc.getIdentity())) {
                allTableNames.add(tableDesc.getIdentity());
                allTables.add(tableDesc);
            }
        }

        // indexing for each type of columns
        bitmapCols = new int[DimensionDesc.getColumnCount(bitmapDimensions)];
        valueCols = new int[DimensionDesc.getColumnCount(valueDimensions)];
        metricsCols = new int[metricNames.length];
        metricsColSet = new BitSet(this.getTableDesc(this.getFactTableName()).getColumnCount());

        int totalIndex = 0;
        for (int i = 0; i < bitmapCols.length; ++i, ++totalIndex) {
            bitmapCols[i] = totalIndex;
        }
        for (int i = 0; i < valueCols.length; ++i, ++totalIndex) {
            valueCols[i] = totalIndex;
        }
        for (int i = 0; i < metricsCols.length; ++i, ++totalIndex) {
            metricsCols[i] = totalIndex;
            metricsColSet.set(totalIndex);
        }

        // partitioning column
        tsCol = -1;
        for (int i = 0; i < allColumns.size(); ++i) {
            TblColRef col = allColumns.get(i);

            if (col.isSameAs(this.getFactTableName(), this.timestampDimension)) {
                tsCol = i;
                break;
            }
        }
        if (tsCol < 0)
            throw new RuntimeException("timestamp_dimension is not in bitmapDimensions or valueDimensions");
    }

    private TableDesc getTableDesc(String tableName) {
        return MetadataManager.getInstance(this.config).getTableDesc(tableName);
    }

    public String getResourcePath() {
        return getIIDescResourcePath(name);
    }

    public static String getIIDescResourcePath(String descName) {
        return ResourceStore.II_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    public List<MeasureDesc> getMeasures() {
        return measureDescs;
    }

    public List<FunctionDesc> listAllFunctions() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();
        for (MeasureDesc m : measureDescs) {
            functions.add(m.getFunction());
        }
        return functions;
    }

    private MeasureDesc makeNormalMeasure(String func, ColumnDesc columnDesc) {
        String columnName = columnDesc.getName();
        String returnType = columnDesc.getTypeName();
        MeasureDesc measureDesc = new MeasureDesc();
        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression(func);
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue(columnName);
        p1.setColRefs(ImmutableList.of(new TblColRef(columnDesc)));
        f1.setParameter(p1);
        f1.setReturnType(returnType);
        measureDesc.setFunction(f1);
        return measureDesc;
    }

    /**
     * 
     * @param hllType represents the presision
     */
    private MeasureDesc makeHLLMeasure(ColumnDesc columnDesc, String hllType) {
        String columnName = columnDesc.getName();
        MeasureDesc measureDesc = new MeasureDesc();
        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("COUNT_DISTINCT");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue(columnName);
        p1.setColRefs(ImmutableList.of(new TblColRef(columnDesc)));
        f1.setParameter(p1);
        f1.setReturnType(hllType);
        measureDesc.setFunction(f1);
        return measureDesc;
    }

    private MeasureDesc makeCountMeasure() {
        MeasureDesc measureDesc = new MeasureDesc();
        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("COUNT");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("constant");
        p1.setValue("1");
        f1.setParameter(p1);
        f1.setReturnType("bigint");
        measureDesc.setFunction(f1);
        return measureDesc;
    }

    /**
     * at first stage the only table in II is fact table, tables
     * 
     * @return
     */
    public List<TableDesc> listTables() {
        return allTables;
    }

    public List<TblColRef> listAllColumns() {
        return allColumns;
    }

    public List<TblColRef> listAllDimensions() {
        return allDimensions;
    }

    public TblColRef findColumnRef(String table, String column) {
        ColumnDesc columnDesc = this.getTableDesc(table).findColumnByName(column);
        return new TblColRef(columnDesc);
    }

    public int findColumn(TblColRef col) {
        return this.allColumns.indexOf(col);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public String getName() {
        return name;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public DataModelDesc getModel() {
        return model;
    }

    public void setModel(DataModelDesc model) {
        this.model = model;
    }

    public int getTimestampColumn() {
        return tsCol;
    }

    public int[] getBitmapColumns() {
        return bitmapCols;
    }

    public int[] getValueColumns() {
        return valueCols;
    }

    public int[] getMetricsColumns() {
        return metricsCols;
    }

    public short getSharding() {
        return sharding;
    }

    public int getSliceSize() {
        return sliceSize;
    }


    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public boolean isMetricsCol(TblColRef col) {
        if (!col.getTable().equalsIgnoreCase(this.getFactTableName()))
            return false;
        return isMetricsCol(this.findColumn(col));
    }

    public boolean isMetricsCol(int index) {
        return metricsColSet.get(index);
    }

    /**
     * the returned fact table name is guaranteed to be in the form of db.table
     * 
     * @return
     */
    public String getFactTableName() {
        return this.model.getFactTable().toUpperCase();
    }

    public String getTimestampDimension() {
        return timestampDimension;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String calculateSignature() {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            StringBuilder sigString = new StringBuilder();
            sigString.append(this.name).append("|").append(this.getFactTableName()).append("|").append(timestampDimension).append("|").append(JsonUtil.writeValueAsString(this.bitmapDimensions)).append("|").append(JsonUtil.writeValueAsString(valueDimensions)).append("|").append(JsonUtil.writeValueAsString(this.metricNames)).append("|").append(sharding).append("|").append(sliceSize);

            byte[] signature = md.digest(sigString.toString().getBytes());
            return new String(Base64.encodeBase64(signature));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate signature");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to calculate signature");
        }

    }

}
