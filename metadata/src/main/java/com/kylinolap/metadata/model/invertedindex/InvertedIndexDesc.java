package com.kylinolap.metadata.model.invertedindex;

import java.util.BitSet;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.kylinolap.metadata.model.cube.MeasureDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.ParameterDesc;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.RootPersistentEntity;
import com.kylinolap.common.util.StringUtil;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.realization.TblColRef;

/**
 * @author yangli9
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class InvertedIndexDesc extends RootPersistentEntity {

    public static final String HBASE_FAMILY = "f";
    public static final String HBASE_QUALIFIER = "c";
    public static final byte[] HBASE_FAMILY_BYTES = Bytes.toBytes(HBASE_FAMILY);
    public static final byte[] HBASE_QUALIFIER_BYTES = Bytes.toBytes(HBASE_QUALIFIER);

    private KylinConfig config;

    @JsonProperty("name")
    private String name;
    @JsonProperty("fact_table")
    private String factTable;
    @JsonProperty("timestamp_dimension")
    private String timestampDimension;
    @JsonProperty("bitmap_dimensions")
    private String[] bitmapDimensions;
    @JsonProperty("value_dimensions")
    private String[] valueDimensions;
    @JsonProperty("metrics")
    private String[] metrics;
    @JsonProperty("sharding")
    private short sharding = 1; // parallelism
    @JsonProperty("slice_size")
    private int sliceSize = 50000; // no. rows

    // computed
    private TableDesc tableDesc;
    private int tsCol;
    private int[] bitmapCols;
    private int[] valueCols;
    private int[] metricsCols;
    private BitSet metricsColSet;
    private List<MeasureDesc> measureDescs;


    public void init(MetadataManager mgr) {
        config = mgr.getConfig();

        factTable = factTable.toUpperCase();
        timestampDimension = timestampDimension.toUpperCase();
        StringUtil.toUpperCaseArray(bitmapDimensions, bitmapDimensions);
        StringUtil.toUpperCaseArray(valueDimensions, valueDimensions);
        StringUtil.toUpperCaseArray(metrics, metrics);

        tableDesc = mgr.getTableDesc(factTable);
        bitmapCols = new int[bitmapDimensions.length];
        valueCols = new int[valueDimensions.length];
        metricsCols = new int[metrics.length];
        metricsColSet = new BitSet(tableDesc.getColumnCount());
        measureDescs = Lists.newArrayList();
        int i = 0, j = 0, k = 0;
        for (ColumnDesc col : tableDesc.getColumns()) {
            if (ArrayUtils.contains(bitmapDimensions, col.getName())) {
                bitmapCols[i++] = col.getZeroBasedIndex();
            }
            if (ArrayUtils.contains(valueDimensions, col.getName())) {
                valueCols[j++] = col.getZeroBasedIndex();
            }
            if (ArrayUtils.contains(metrics, col.getName())) {
                metricsCols[k++] = col.getZeroBasedIndex();
                metricsColSet.set(col.getZeroBasedIndex());
                measureDescs.add(makeMeasureDescs("SUM", col));
                measureDescs.add(makeMeasureDescs("MIN", col));
                measureDescs.add(makeMeasureDescs("MAX", col));
                //TODO support for HLL
            }
        }

        tsCol = tableDesc.findColumnByName(timestampDimension).getZeroBasedIndex();
    }

    public List<MeasureDesc> getMeasureDescs() {
        return measureDescs;
    }

    private MeasureDesc makeMeasureDescs(String func, ColumnDesc columnDesc) {
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

    public KylinConfig getConfig() {
        return config;
    }

    public String getName() {
        return name;
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

    public boolean isMetricsCol(TblColRef col) {
        assert col.getTable().equals(factTable);
        return isMetricsCol(col.getColumn().getZeroBasedIndex());
    }

    public boolean isMetricsCol(int colZeroBasedIndex) {
        return metricsColSet.get(colZeroBasedIndex);
    }

    public String getResourcePath() {
        return ResourceStore.IIDESC_RESOURCE_ROOT + "/" + name + ".json";
    }

    public TableDesc getFactTableDesc() {
        return tableDesc;
    }

    public String getFactTable() {
        return factTable;
    }

    public String getTimestampDimension() {
        return timestampDimension;
    }

}
