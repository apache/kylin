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

package org.apache.kylin.job;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import static org.apache.kylin.job.util.FlatTableSqlQuoteUtils.quoteIdentifier;
import static org.apache.kylin.job.util.FlatTableSqlQuoteUtils.quoteTableIdentity;
import static org.apache.kylin.job.util.FlatTableSqlQuoteUtils.quoteIdentifierInSqlExpr;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class JoinedFlatTable {
    public static final String TEXTFILE = "TEXTFILE";
    public static final String SEQUENCEFILE = "SEQUENCEFILE";

    public static String getTableDir(IJoinedFlatTableDesc flatDesc, String storageDfsDir) {
        return storageDfsDir + "/" + flatDesc.getTableName();
    }

    public static String generateHiveInitStatements(String flatTableDatabase) {

        StringBuilder buffer = new StringBuilder();
        buffer.append("USE ").append(flatTableDatabase).append(";\n");
        return buffer.toString();
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc, String storageDfsDir) {
        String storageFormat = flatDesc.getSegment().getConfig().getFlatTableStorageFormat();
        return generateCreateTableStatement(flatDesc, storageDfsDir, storageFormat);
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc, String storageDfsDir,
            String storageFormat) {
        String fieldDelimiter = flatDesc.getDataModel().getConfig().getFlatTableFieldDelimiter();
        return generateCreateTableStatement(flatDesc, storageDfsDir, storageFormat, fieldDelimiter);
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc, String storageDfsDir,
            String storageFormat, String fieldDelimiter) {
        StringBuilder ddl = new StringBuilder();

        ddl.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + flatDesc.getTableName() + "\n");

        ddl.append("(" + "\n");
        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(quoteIdentifier(colName(col, flatDesc.useAlias()), null) + " " + getHiveDataType(col.getDatatype()) + "\n");
        }
        ddl.append(")" + "\n");
        if (TEXTFILE.equals(storageFormat)) {
            ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fieldDelimiter + "'\n");
        }
        ddl.append("STORED AS " + storageFormat + "\n");
        ddl.append("LOCATION '" + getTableDir(flatDesc, storageDfsDir) + "';").append("\n");
        ddl.append("ALTER TABLE " + flatDesc.getTableName() + " SET TBLPROPERTIES('auto.purge'='true');\n");
        return ddl.toString();
    }

    public static String generateDropTableStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("DROP TABLE IF EXISTS " + flatDesc.getTableName() + ";").append("\n");
        return ddl.toString();
    }

    public static String generateInsertDataStatement(IJoinedFlatTableDesc flatDesc) {
        CubeSegment segment = ((CubeSegment) flatDesc.getSegment());
        KylinConfig kylinConfig;
        if (null == segment) {
            kylinConfig = KylinConfig.getInstanceFromEnv();
        } else {
            kylinConfig = (flatDesc.getSegment()).getConfig();
        }

        if (kylinConfig.isAdvancedFlatTableUsed()) {
            try {
                Class advancedFlatTable = Class.forName(kylinConfig.getAdvancedFlatTableClass());
                Method method = advancedFlatTable.getMethod("generateInsertDataStatement", IJoinedFlatTableDesc.class,
                        JobEngineConfig.class);
                return (String) method.invoke(null, flatDesc);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return "INSERT OVERWRITE TABLE " + quoteIdentifier(flatDesc.getTableName(), null) + " " + generateSelectDataStatement(flatDesc)
                + ";\n";
    }

    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc) {
        return generateSelectDataStatement(flatDesc, false, null, null);
    }

    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc, boolean singleLine,
                                                     String[] skipAs, SqlDialect sqlDialect) {
        final String sep = singleLine ? " " : "\n";
        final List<String> skipAsList = (skipAs == null) ? new ArrayList<>() : Arrays.asList(skipAs);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + sep);

        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                sql.append(",");
            }
            String colTotalName = String.format(Locale.ROOT, "%s.%s", col.getTableRef().getTableName(), col.getName());
            String quotedColTotalName = String.format(Locale.ROOT, "%s.%s",
                    quoteIdentifier(col.getTableAlias(), sqlDialect),
                    quoteIdentifier(col.getName(), sqlDialect));
            if (skipAsList.contains(colTotalName)) {
                sql.append(quotedColTotalName).append(sep);
            } else {
                sql.append(quotedColTotalName).append(" as ").append(quoteIdentifier(colName(col), sqlDialect))
                        .append(sep);
            }
        }
        appendJoinStatement(flatDesc, sql, singleLine, sqlDialect);
        appendWhereStatement(flatDesc, sql, singleLine, sqlDialect);
        return sql.toString();
    }

    static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine, SqlDialect sqlDialect) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = new HashSet<>();

        DataModelDesc model = flatDesc.getDataModel();
        TableRef rootTable = model.getRootFactTable();
        sql.append(" FROM ").append(quoteTableIdentity(flatDesc.getDataModel().getRootFactTable(), sqlDialect))
                .append(" as ").append(quoteIdentifier(rootTable.getAlias(), sqlDialect)).append(sep);

        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            if (join != null && join.getType().equals("") == false) {
                TableRef dimTable = lookupDesc.getTableRef();
                if (!dimTableCache.contains(dimTable)) {
                    TblColRef[] pk = join.getPrimaryKeyColumns();
                    TblColRef[] fk = join.getForeignKeyColumns();
                    if (pk.length != fk.length) {
                        throw new RuntimeException("Invalid join condition of lookup table:" + lookupDesc);
                    }
                    String joinType = join.getType().toUpperCase(Locale.ROOT);
                    sql.append(joinType).append(" JOIN ").append(quoteTableIdentity(dimTable, sqlDialect))
                            .append(" as ").append(quoteIdentifier(dimTable.getAlias(), sqlDialect)).append(sep);
                    sql.append("ON ");
                    for (int i = 0; i < pk.length; i++) {
                        if (i > 0) {
                            sql.append(" AND ");
                        }
                        sql.append(getQuotedColExpressionInSourceDB(flatDesc, fk[i], sqlDialect)).append(" = ")
                                .append(getQuotedColExpressionInSourceDB(flatDesc, pk[i], sqlDialect));
                    }
                    sql.append(sep);

                    dimTableCache.add(dimTable);
                }
            }
        }
    }

    private static void appendDistributeStatement(StringBuilder sql, List<TblColRef> redistCols) {
        sql.append(" DISTRIBUTE BY ");
        for (TblColRef redistCol : redistCols) {
            sql.append(colName(redistCol, true)).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(";\n");
    }

    private static void appendClusterStatement(StringBuilder sql, TblColRef clusterCol) {
        sql.append(" CLUSTER BY CAST(").append(colName(clusterCol)).append(" AS STRING);\n");
    }

    private static void appendWhereStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine, SqlDialect sqlDialect) {
        final String sep = singleLine ? " " : "\n";

        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("WHERE 1=1");

        DataModelDesc model = flatDesc.getDataModel();
        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            String filterCondition = model.getFilterCondition();
            if (flatDesc.getSegment() != null) {
                JoinedFormatter formatter = new JoinedFormatter(flatDesc);
                filterCondition = formatter.formatSentence(model.getFilterCondition());
            }
            String quotedFilterCondition = quoteIdentifierInSqlExpr(flatDesc, filterCondition, null);
            whereBuilder.append(" AND (").append(quotedFilterCondition).append(") "); // -> filter condition contains special character may cause bug
        }
        if (flatDesc.getSegment() != null) {
            PartitionDesc partDesc = model.getPartitionDesc();
            if (partDesc != null && partDesc.getPartitionDateColumn() != null) {
                SegmentRange segRange = flatDesc.getSegRange();

                if (segRange != null && !segRange.isInfinite()) {
                    whereBuilder.append(" AND (");
                    String quotedPartitionCond = quoteIdentifierInSqlExpr(flatDesc,
                            partDesc.getPartitionConditionBuilder().buildDateRangeCondition(partDesc, flatDesc.getSegment(), segRange, null), sqlDialect);
                    whereBuilder.append(quotedPartitionCond);
                    whereBuilder.append(")" + sep);
                }
            }
        }

        sql.append(whereBuilder.toString());
    }

    public static String colName(TblColRef col) {
        return colName(col, true);
    }

    public static String colName(TblColRef col, boolean useAlias) {
        return useAlias ? col.getTableAlias() + "_" + col.getName() : col.getName();
    }

    private static String getHiveDataType(String javaDataType) {
        String originDataType = javaDataType.toLowerCase(Locale.ROOT);
        String hiveDataType;
        if (originDataType.startsWith("varchar")) {
            hiveDataType = "string";
        } else if (originDataType.startsWith("integer")) {
            hiveDataType = "int";
        } else if (originDataType.startsWith("bigint")) {
            hiveDataType = "bigint";
        } else if (originDataType.startsWith("double")) {
            hiveDataType = "double";
        } else if (originDataType.startsWith("float")) {
            hiveDataType = "float";
        } else {
            hiveDataType = originDataType;
        }

        return hiveDataType;
    }

    public static String generateRedistributeFlatTableStatement(IJoinedFlatTableDesc flatDesc, CubeDesc cubeDesc) {
        final String tableName = flatDesc.getTableName();
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT OVERWRITE TABLE " + quoteIdentifier(tableName, null) + " SELECT * FROM " + quoteIdentifier(tableName, null));

        if (flatDesc.getClusterBy() != null) {
            appendClusterStatement(sql, flatDesc.getClusterBy());
        } else if (flatDesc.getDistributedBy() != null) {
            appendDistributeStatement(sql, Lists.newArrayList(flatDesc.getDistributedBy()));
        } else {
            int redistColumnCount = cubeDesc.getConfig().getHiveRedistributeColumnCount();

            RowKeyColDesc[] rowKeyColDescs = cubeDesc.getRowkey().getRowKeyColumns();

            if (rowKeyColDescs.length < redistColumnCount)
                redistColumnCount = rowKeyColDescs.length;

            List<TblColRef> redistColumns = Lists.newArrayListWithCapacity(redistColumnCount);

            for (int i = 0; i < redistColumnCount; i++) {
                redistColumns.add(rowKeyColDescs[i].getColRef());
            }

            appendDistributeStatement(sql, redistColumns);
        }

        return sql.toString();
    }

    public static String getQuotedColExpressionInSourceDB(IJoinedFlatTableDesc flatDesc, TblColRef col, SqlDialect sqlDialect) {
        if (!col.getColumnDesc().isComputedColumn()) {
            return quoteIdentifier(col.getTableAlias(), sqlDialect) + "."
                    + quoteIdentifier(col.getName(), sqlDialect);
        } else {
            String computeExpr = col.getColumnDesc().getComputedColumnExpr();
            return quoteIdentifierInSqlExpr(flatDesc, computeExpr, sqlDialect);
        }
    }
}
