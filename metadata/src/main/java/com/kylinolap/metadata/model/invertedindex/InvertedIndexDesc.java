package com.kylinolap.metadata.model.invertedindex;

import java.util.BitSet;

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
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;

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
            }
        }

        tsCol = tableDesc.findColumnByName(timestampDimension).getZeroBasedIndex();
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
