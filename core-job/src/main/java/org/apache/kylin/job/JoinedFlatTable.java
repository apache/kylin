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

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 *
 */

public class JoinedFlatTable {

    public static String getTableDir(IJoinedFlatTableDesc flatDesc, String storageDfsDir) {
        return storageDfsDir + "/" + flatDesc.getTableName();
    }

    public static String generateHiveInitStatements(
            String flatTableDatabase, String kylinHiveFile, Map<String, String> cubeOverrides) {

        StringBuilder buffer = new StringBuilder();

        buffer.append("USE ").append(flatTableDatabase).append(";\n");
        try {
            File file = new File(kylinHiveFile);
            if (file.exists()) {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(file);
                NodeList nl = doc.getElementsByTagName("property");
                for (int i = 0; i < nl.getLength(); i++) {
                    String name = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                    String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                    if (!name.equals("tmpjars")) {
                        buffer.append("SET ").append(name).append("=").append(value).append(";\n");
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse hive conf file ", e);
        }

        for (Map.Entry<String, String> entry : cubeOverrides.entrySet()) {
            buffer.append("SET ").append(entry.getKey()).append("=").append(entry.getValue()).append(";\n");
        }

        return buffer.toString();
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc, String storageDfsDir) {
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
        ddl.append("STORED AS SEQUENCEFILE" + "\n");
        ddl.append("LOCATION '" + getTableDir(flatDesc, storageDfsDir) + "';").append("\n");
        return ddl.toString();
    }

    public static String generateDropTableStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("DROP TABLE IF EXISTS " + flatDesc.getTableName() + ";").append("\n");
        return ddl.toString();
    }

    public static String generateInsertDataStatement(IJoinedFlatTableDesc flatDesc) {
        return "INSERT OVERWRITE TABLE " + flatDesc.getTableName() + " " + generateSelectDataStatement(flatDesc) + ";\n";
    }

    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + "\n");
        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                sql.append(",");
            }
            sql.append(col.getTableAlias() + "." + col.getName() + "\n");
        }
        appendJoinStatement(flatDesc, sql);
        appendWhereStatement(flatDesc, sql);
        return sql.toString();
    }

    public static String generateCountDataStatement(IJoinedFlatTableDesc flatDesc, final String outputDir) {
        final StringBuilder sql = new StringBuilder();
        final TableRef rootTbl = flatDesc.getDataModel().getRootFactTable();
        sql.append("dfs -mkdir -p " + outputDir + ";\n");
        sql.append("INSERT OVERWRITE DIRECTORY '" + outputDir + "' SELECT count(*) FROM " + rootTbl.getTableIdentity() + " " + rootTbl.getAlias() + "\n");
        appendWhereStatement(flatDesc, sql);
        return sql.toString();
    }

    private static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql) {
        Set<TableRef> dimTableCache = new HashSet<>();

        DataModelDesc model = flatDesc.getDataModel();
        TableRef rootTable = model.getRootFactTable();
        sql.append("FROM " + rootTable.getTableIdentity() + " as " + rootTable.getAlias() + " \n");

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
                    sql.append(joinType + " JOIN " + dimTable.getTableIdentity() + " as " + dimTable.getAlias() + "\n");
                    sql.append("ON ");
                    for (int i = 0; i < pk.length; i++) {
                        if (i > 0) {
                            sql.append(" AND ");
                        }
                        sql.append(fk[i].getTableAlias() + "." + fk[i].getName() + " = " + pk[i].getTableAlias() + "." + pk[i].getName());
                    }
                    sql.append("\n");

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
                long dateStart = flatDesc.getSourceOffsetStart();
                long dateEnd = flatDesc.getSourceOffsetEnd();

                if (!(dateStart == 0 && dateEnd == Long.MAX_VALUE)) {
                    whereBuilder.append(hasCondition ? " AND (" : " (");
                    whereBuilder.append(partDesc.getPartitionConditionBuilder().buildDateRangeCondition(partDesc, dateStart, dateEnd));
                    whereBuilder.append(")\n");
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
