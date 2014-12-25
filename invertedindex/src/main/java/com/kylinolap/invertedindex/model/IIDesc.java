package com.kylinolap.invertedindex.model;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.metadata.model.*;
import com.kylinolap.metadata.model.ParameterDesc;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.RootPersistentEntity;
import com.kylinolap.common.util.StringUtil;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author yangli9
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class IIDesc extends RootPersistentEntity {

	public static final String HBASE_FAMILY = "f";
	public static final String HBASE_QUALIFIER = "c";
	public static final byte[] HBASE_FAMILY_BYTES = Bytes.toBytes(HBASE_FAMILY);
	public static final byte[] HBASE_QUALIFIER_BYTES = Bytes
			.toBytes(HBASE_QUALIFIER);

	private KylinConfig config;
	private DataModelDesc model;

	@JsonProperty("name")
	private String name;
	@JsonProperty("model_name")
	private String modelName;
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
	@JsonProperty("signature")
	private String signature;

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

		if (this.modelName == null || this.modelName.length() == 0) {
			throw new RuntimeException("The cubeDesc '" + this.getName()
					+ "' doesn't have data model specified.");
		}

		this.model = MetadataManager.getInstance(config).getDataModelDesc(
				this.modelName);

		if (this.model == null) {
			throw new RuntimeException("No data model found with name '"
					+ modelName + "'.");
		}

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
				// TODO support for HLL
			}
		}

		tsCol = tableDesc.findColumnByName(timestampDimension)
				.getZeroBasedIndex();
	}

	public String getResourcePath() {
		return getIIDescResourcePath(name);
	}

	public static String getIIDescResourcePath(String descName) {
		return ResourceStore.II_DESC_RESOURCE_ROOT + "/" + descName
				+ MetadataConstances.FILE_SURFIX;
	}

	public List<MeasureDesc> getMeasures() {
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

	/**
	 * at first stage the only table in II is fact table, TODO: to extend to all
	 * tables
	 * 
	 * @return
	 */
	public List<TableDesc> listTables() {
		return Lists.newArrayList(this.tableDesc);
	}

	public List<TblColRef> listAllColumns() {
		List<TblColRef> ret = Lists.newArrayList();
		for (ColumnDesc columnDesc : this.tableDesc.getColumns()) {
			ret.add(new TblColRef(columnDesc));
		}
		return ret;
	}

	public TblColRef findColumnRef(String table, String column) {
		ColumnDesc columnDesc = this.tableDesc.findColumnByName(column);
		return new TblColRef(columnDesc);
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
		assert col.getTable().equals(factTable);
		return isMetricsCol(col.getColumn().getZeroBasedIndex());
	}

	public boolean isMetricsCol(int colZeroBasedIndex) {
		return metricsColSet.get(colZeroBasedIndex);
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

	public String calculateSignature() {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
			StringBuilder sigString = new StringBuilder();
			sigString.append(this.name).append("|").append(this.getFactTable())
					.append("|").append(timestampDimension).append("|")
					.append(JsonUtil.writeValueAsString(this.bitmapDimensions))
					.append("|")
					.append(JsonUtil.writeValueAsString(valueDimensions))
					.append("|")
					.append(JsonUtil.writeValueAsString(this.metrics))
					.append("|").append(sharding).append("|").append(sliceSize);

			byte[] signature = md.digest(sigString.toString().getBytes());
			return new String(Base64.encodeBase64(signature));
		} catch (NoSuchAlgorithmException | JsonProcessingException e) {
			throw new RuntimeException("Failed to calculate signature");
		}
	}

}
