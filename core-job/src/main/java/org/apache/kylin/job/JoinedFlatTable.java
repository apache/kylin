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
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

/**
 *
 */

public class JoinedFlatTable {

    public static String getTableDir(IJoinedFlatTableDesc flatDesc, String storageDfsDir) {
        return storageDfsDir + "/" + flatDesc.getTableName();
    }

    public static String generateHiveInitStatements(String flatTableDatabase) {

        StringBuilder buffer = new StringBuilder();
        buffer.append("USE ").append(flatTableDatabase).append(";\n");
        return buffer.toString();
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc, String storageDfsDir) {
        return generateCreateTableStatement(flatDesc, storageDfsDir, "SEQUENCEFILE");
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc, String storageDfsDir,
            String format) {
        StringBuilder ddl = new StringBuilder();

        ddl.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + flatDesc.getTableName() + "\n");

        ddl.append("(" + "\n");
        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(colName(col) + " " + getHiveDataType(col.getDatatype()) + "\n");
        }
        ddl.append(")" + "\n");
        if ("TEXTFILE".equals(format)) {
            ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + "\n");
        }
        ddl.append("STORED AS " + format + "\n");
        ddl.append("LOCATION '" + getTableDir(flatDesc, storageDfsDir) + "';").append("\n");
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
            kylinConfig = ((CubeSegment) flatDesc.getSegment()).getConfig();
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

        return "INSERT OVERWRITE TABLE " + flatDesc.getTableName() + " " + generateSelectDataStatement(flatDesc)
                + ";\n";
    }

    public static String generateInsertPartialDataStatement(IJoinedFlatTableDesc flatDesc) {
        return "INSERT OVERWRITE TABLE " + flatDesc.getTableName() + " " + generateSelectDataStatement(flatDesc)
                + ";\n";
    }

    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc) {
        return generateSelectDataStatement(flatDesc, false, null);
    }

    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc, boolean singleLine,
            String[] skipAs) {
        final String sep = singleLine ? " " : "\n";
        final List<String> skipAsList = (skipAs == null) ? new ArrayList<String>() : Arrays.asList(skipAs);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + sep);

        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                sql.append(",");
            }
            String colTotalName = String.format("%s.%s", col.getTableRef().getTableName(), col.getName());
            if (skipAsList.contains(colTotalName)) {
                sql.append(col.getExpressionInSourceDB() + sep);
            } else {
                sql.append(col.getExpressionInSourceDB() + " as " + colName(col) + sep);
            }
        }
        appendJoinStatement(flatDesc, sql, singleLine);
        appendWhereStatement(flatDesc, sql, singleLine);
        return sql.toString();
    }

    public static String generateCountDataStatement(IJoinedFlatTableDesc flatDesc, final String outputDir) {
        final StringBuilder sql = new StringBuilder();
        final TableRef rootTbl = flatDesc.getDataModel().getRootFactTable();
        sql.append("dfs -mkdir -p " + outputDir + ";\n");
        sql.append("INSERT OVERWRITE DIRECTORY '" + outputDir + "' SELECT count(*) FROM " + rootTbl.getTableIdentity()
                + " " + rootTbl.getAlias() + "\n");
        appendWhereStatement(flatDesc, sql);
        return sql.toString();
    }

    public static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = new HashSet<>();

        DataModelDesc model = flatDesc.getDataModel();
        TableRef rootTable = model.getRootFactTable();
        sql.append("FROM " + rootTable.getTableIdentity() + " as " + rootTable.getAlias() + " " + sep);

        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            if (join != null && join.getType().equals("") == false) {
                String joinType = join.getType().toUpperCase();
                TableRef dimTable = lookupDesc.getTableRef();
                if (!dimTableCache.contains(dimTable)) {
                    TblColRef[] pk = join.getPrimaryKeyColumns();
                    TblColRef[] fk = join.getForeignKeyColumns();
                    if (pk.length != fk.length) {
                        throw new RuntimeException("Invalid join condition of lookup table:" + lookupDesc);
                    }
                    sql.append(joinType + " JOIN " + dimTable.getTableIdentity() + " as " + dimTable.getAlias() + sep);
                    sql.append("ON ");
                    for (int i = 0; i < pk.length; i++) {
                        if (i > 0) {
                            sql.append(" AND ");
                        }
                        sql.append(fk[i].getIdentity() + " = " + pk[i].getIdentity());
                    }
                    sql.append(sep);

                    dimTableCache.add(dimTable);
                }
            }
        }
    }

    private static void appendDistributeStatement(StringBuilder sql, TblColRef redistCol) {
        if (redistCol != null) {
            sql.append(" DISTRIBUTE BY ").append(colName(redistCol)).append(";\n");
        } else {
            sql.append(" DISTRIBUTE BY RAND()").append(";\n");
        }
    }

    private static void appendClusterStatement(StringBuilder sql, TblColRef clusterCol) {
        sql.append(" CLUSTER BY ").append(colName(clusterCol)).append(";\n");
    }

    private static void appendWhereStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql) {
        appendWhereStatement(flatDesc, sql, false);
    }

    private static void appendWhereStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";

        boolean hasCondition = false;
        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("WHERE");

        DataModelDesc model = flatDesc.getDataModel();

        if (model.getFilterCondition() != null && model.getFilterCondition().equals("") == false) {
            whereBuilder.append(" (").append(model.getFilterCondition()).append(") ");
            hasCondition = true;
        }

        if (flatDesc.getSegment() != null) {
            PartitionDesc partDesc = model.getPartitionDesc();
            if (partDesc != null && partDesc.getPartitionDateColumn() != null) {
                SegmentRange segRange = flatDesc.getSegRange();

                if (segRange != null && !segRange.isInfinite()) {
                    whereBuilder.append(hasCondition ? " AND (" : " (");
                    whereBuilder.append(
                            partDesc.getPartitionConditionBuilder().buildDateRangeCondition(partDesc, segRange));
                    whereBuilder.append(")" + sep);
                    hasCondition = true;
                }
            }
        }

        if (hasCondition) {
            sql.append(whereBuilder.toString());
        }
    }

    private static String colName(TblColRef col) {
        return col.getTableAlias() + "_" + col.getName();
    }

    private static String getHiveDataType(String javaDataType) {
        String hiveDataType = javaDataType.toLowerCase().startsWith("varchar") ? "string" : javaDataType;
        hiveDataType = javaDataType.toLowerCase().startsWith("integer") ? "int" : hiveDataType;

        return hiveDataType.toLowerCase();
    }

    public static String generateRedistributeFlatTableStatement(IJoinedFlatTableDesc flatDesc) {
        final String tableName = flatDesc.getTableName();
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName);

        TblColRef clusterCol = flatDesc.getClusterBy();
        if (clusterCol != null) {
            appendClusterStatement(sql, clusterCol);
        } else {
            appendDistributeStatement(sql, flatDesc.getDistributedBy());
        }

        return sql.toString();
    }

}
