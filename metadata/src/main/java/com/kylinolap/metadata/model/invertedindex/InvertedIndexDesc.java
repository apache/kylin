package com.kylinolap.metadata.model.invertedindex;

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
    @JsonProperty("low_cardinality_dimensions")
    private String[] lowCardinalityDimensions;
    @JsonProperty("high_cardinality_dimensions")
    private String[] highCardinalityDimensions;
    @JsonProperty("measures")
    private String[] measures;

    @JsonProperty("timestamp_granularity")
    private int timestampGranularity = 1000 * 60 * 60 * 24; // milliseconds
    @JsonProperty("slice_length")
    private int sliceLength = 102400; // bytes

    // computed
    private int tsCol;
    private int[] bitmapCols;
    private int[] valueCols;

    public void init(MetadataManager mgr) {
        config = mgr.getConfig();

        factTable = factTable.toUpperCase();
        timestampDimension = timestampDimension.toUpperCase();
        StringUtil.toUpperCaseArray(lowCardinalityDimensions, lowCardinalityDimensions);
        StringUtil.toUpperCaseArray(highCardinalityDimensions, highCardinalityDimensions);
        StringUtil.toUpperCaseArray(measures, measures);

        bitmapCols = new int[lowCardinalityDimensions.length];
        valueCols = new int[highCardinalityDimensions.length + measures.length];
        int i = 0, j = 0;
        TableDesc tableDesc = mgr.getTableDesc(factTable);
        for (ColumnDesc col : tableDesc.getColumns()) {
            if (ArrayUtils.contains(lowCardinalityDimensions, col.getName())) {
                bitmapCols[i++] = col.getZeroBasedIndex();
            }
            if (ArrayUtils.contains(highCardinalityDimensions, col.getName())
                    || ArrayUtils.contains(measures, col.getName())) {
                valueCols[j++] = col.getZeroBasedIndex();
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

    public String getResourcePath() {
        return ResourceStore.IIDESC_RESOURCE_ROOT + "/" + name + ".json";
    }

    public TableDesc getFactTableDesc() {
        return MetadataManager.getInstance(config).getTableDesc(factTable);
    }

    public String getFactTable() {
        return factTable;
    }

    public void setFactTable(String factTable) {
        this.factTable = factTable;
    }

    public String getTimestampDimension() {
        return timestampDimension;
    }

    public void setTimestampDimension(String timestampDimension) {
        this.timestampDimension = timestampDimension;
    }

    public String[] getLowCardinalityDimensions() {
        return lowCardinalityDimensions;
    }

    public void setLowCardinalityDimensions(String[] lowCardinalityDimensions) {
        this.lowCardinalityDimensions = lowCardinalityDimensions;
    }

    public String[] getHighCardinalityDimensions() {
        return highCardinalityDimensions;
    }

    public void setHighCardinalityDimensions(String[] highCardinalityDimensions) {
        this.highCardinalityDimensions = highCardinalityDimensions;
    }

    public String[] getMeasures() {
        return measures;
    }

    public void setMeasures(String[] measures) {
        this.measures = measures;
    }

    public int getTimestampGranularity() {
        return timestampGranularity;
    }

    public void setTimestampGranularity(int timestampGranularity) {
        this.timestampGranularity = timestampGranularity;
    }

    public int getSliceLength() {
        return sliceLength;
    }

    public void setSliceLength(int partitionLength) {
        this.sliceLength = partitionLength;
    }
}
