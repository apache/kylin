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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 *
 */

public class JoinedFlatTable {

    public static String getTableDir(IJoinedFlatTableDesc intermediateTableDesc, String storageDfsDir) {
        return storageDfsDir + "/" + intermediateTableDesc.getTableName();
    }

    public static String generateHiveSetStatements(JobEngineConfig engineConfig) {
        StringBuilder buffer = new StringBuilder();

        try {
            File hadoopPropertiesFile = new File(engineConfig.getHiveConfFilePath());

            if (hadoopPropertiesFile.exists()) {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder;
                Document doc;
                builder = factory.newDocumentBuilder();
                doc = builder.parse(hadoopPropertiesFile);
                NodeList nl = doc.getElementsByTagName("property");
                for (int i = 0; i < nl.getLength(); i++) {
                    String name = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                    String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                    if (!name.equals("tmpjars")) {
                        buffer.append("SET " + name + "=" + value + ";\n");
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse hive conf file ", e);
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
            ddl.append(colName(col.getCanonicalName()) + " " + getHiveDataType(col.getDatatype()) + "\n");
        }
        ddl.append(")" + "\n");

        ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\177'" + "\n");
        ddl.append("STORED AS SEQUENCEFILE" + "\n");
        ddl.append("LOCATION '" + getTableDir(flatDesc, storageDfsDir) + "';").append("\n");
        // ddl.append("TBLPROPERTIES ('serialization.null.format'='\\\\N')" +
        // ";\n");
        return ddl.toString();
    }

    public static String generateDropTableStatement(IJoinedFlatTableDesc intermediateTableDesc) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("DROP TABLE IF EXISTS " + intermediateTableDesc.getTableName() + ";").append("\n");
        return ddl.toString();
    }

    public static String generateInsertDataStatement(IJoinedFlatTableDesc intermediateTableDesc, JobEngineConfig engineConfig, boolean redistribute) {
        StringBuilder sql = new StringBuilder();
        sql.append(generateHiveSetStatements(engineConfig));
        sql.append("INSERT OVERWRITE TABLE " + intermediateTableDesc.getTableName() + " " + generateSelectDataStatement(intermediateTableDesc, redistribute) + ";").append("\n");
        return sql.toString();
    }

    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc, boolean redistribute) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + "\n");
        String tableAlias;
        Map<String, String> tableAliasMap = buildTableAliasMap(flatDesc.getDataModel());
        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                sql.append(",");
            }
            tableAlias = tableAliasMap.get(col.getTable());
            sql.append(tableAlias + "." + col.getName() + "\n");
        }
        appendJoinStatement(flatDesc, sql, tableAliasMap);
        appendWhereStatement(flatDesc, sql, tableAliasMap);
        if (redistribute == true) {
            String redistributeCol = null;
            TblColRef distDcol = flatDesc.getDistributedBy();
            if (distDcol != null) {
                String tblAlias = tableAliasMap.get(distDcol.getTable());
                redistributeCol = tblAlias + "." + distDcol.getName();
            }
            appendDistributeStatement(sql, redistributeCol);
        }
        return sql.toString();
    }

    public static String generateCountDataStatement(IJoinedFlatTableDesc flatDesc, final String outputDir) {
        final Map<String, String> tableAliasMap = buildTableAliasMap(flatDesc.getDataModel());
        final StringBuilder sql = new StringBuilder();
        final String factTbl = flatDesc.getDataModel().getFactTable();
        sql.append("dfs -mkdir -p " + outputDir + ";\n");
        sql.append("INSERT OVERWRITE DIRECTORY '" + outputDir + "' SELECT count(*) FROM " + factTbl + " " + tableAliasMap.get(factTbl) + "\n");
        appendWhereStatement(flatDesc, sql, tableAliasMap);
        return sql.toString();
    }

    private static Map<String, String> buildTableAliasMap(DataModelDesc dataModelDesc) {
        Map<String, String> tableAliasMap = new HashMap<String, String>();

        addTableAlias(dataModelDesc.getFactTable(), tableAliasMap);

        for (LookupDesc lookupDesc : dataModelDesc.getLookups()) {
            JoinDesc join = lookupDesc.getJoin();
            if (join != null) {
                addTableAlias(lookupDesc.getTable(), tableAliasMap);
            }
        }
        return tableAliasMap;
    }

    // The table alias used to be "FACT_TABLE" and "LOOKUP_#", but that's too unpredictable
    // for those who want to write a filter. (KYLIN-900)
    // Also yet don't support joining the same table more than once, since table name is the map key.
    private static void addTableAlias(String table, Map<String, String> tableAliasMap) {
        String alias;
        int cut = table.lastIndexOf('.');
        if (cut < 0)
            alias = table;
        else
            alias = table.substring(cut + 1);

        tableAliasMap.put(table, alias);
    }

    private static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, Map<String, String> tableAliasMap) {
        List<JoinDesc> cubeJoins = getUsedJoinsSet(flatDesc);

        Set<String> dimTableCache = new HashSet<String>();

        DataModelDesc dataModelDesc = flatDesc.getDataModel();
        String factTableName = dataModelDesc.getFactTable();
        String factTableAlias = tableAliasMap.get(factTableName);
        sql.append("FROM " + factTableName + " as " + factTableAlias + " \n");

        for (LookupDesc lookupDesc : dataModelDesc.getLookups()) {
            JoinDesc join = lookupDesc.getJoin();
            if (!cubeJoins.contains(join)) {
                continue;
            }
            if (join != null && join.getType().equals("") == false) {
                String joinType = join.getType().toUpperCase();
                String dimTableName = lookupDesc.getTable();
                if (!dimTableCache.contains(dimTableName)) {
                    TblColRef[] pk = join.getPrimaryKeyColumns();
                    TblColRef[] fk = join.getForeignKeyColumns();
                    if (pk.length != fk.length) {
                        throw new RuntimeException("Invalid join condition of lookup table:" + lookupDesc);
                    }
                    sql.append(joinType + " JOIN " + dimTableName + " as " + tableAliasMap.get(dimTableName) + "\n");
                    sql.append("ON ");
                    for (int i = 0; i < pk.length; i++) {
                        if (i > 0) {
                            sql.append(" AND ");
                        }
                        sql.append(factTableAlias + "." + fk[i].getName() + " = " + tableAliasMap.get(dimTableName) + "." + pk[i].getName());
                    }
                    sql.append("\n");

                    dimTableCache.add(dimTableName);
                }
            }
        }
    }

    private static List<JoinDesc> getUsedJoinsSet(IJoinedFlatTableDesc flatDesc) {
        Set<String> usedTableIdentities = Sets.newHashSet();
        for (TblColRef col : flatDesc.getAllColumns()) {
            usedTableIdentities.add(col.getTable());
        }
        
        List<JoinDesc> result = Lists.newArrayList();
        for (LookupDesc lookup : flatDesc.getDataModel().getLookups()) {
            String table = lookup.getTableDesc().getIdentity();
            if (usedTableIdentities.contains(table)) {
                result.add(lookup.getJoin());
            }
        }
        
        return result;
    }

    private static void appendDistributeStatement(StringBuilder sql, String redistributeCol) {
        if (redistributeCol != null) {
            sql.append(" DISTRIBUTE BY ").append(redistributeCol).append(";\n");
        } else {
            sql.append(" DISTRIBUTE BY RAND()").append(";\n");
        }
    }

    private static void appendWhereStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, Map<String, String> tableAliasMap) {
        boolean hasCondition = false;
        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("WHERE");

        DataModelDesc model = flatDesc.getDataModel();

        if (model.getFilterCondition() != null && model.getFilterCondition().equals("") == false) {
            whereBuilder.append(" (").append(model.getFilterCondition()).append(") ");
            hasCondition = true;
        }

        PartitionDesc partDesc = model.getPartitionDesc();
        if (partDesc != null && partDesc.getPartitionDateColumn() != null) {
            long dateStart = flatDesc.getSourceOffsetStart();
            long dateEnd = flatDesc.getSourceOffsetEnd();

            if (!(dateStart == 0 && dateEnd == Long.MAX_VALUE)) {
                whereBuilder.append(hasCondition ? " AND (" : " (");
                whereBuilder.append(partDesc.getPartitionConditionBuilder().buildDateRangeCondition(partDesc, dateStart, dateEnd, tableAliasMap));
                whereBuilder.append(")\n");
                hasCondition = true;
            }
        }

        if (hasCondition) {
            sql.append(whereBuilder.toString());
        }
    }

    private static String colName(String canonicalColName) {
        return canonicalColName.replace(".", "_");
    }

    private static String getHiveDataType(String javaDataType) {
        String hiveDataType = javaDataType.toLowerCase().startsWith("varchar") ? "string" : javaDataType;
        hiveDataType = javaDataType.toLowerCase().startsWith("integer") ? "int" : hiveDataType;

        return hiveDataType.toLowerCase();
    }

    public static String generateSelectRowCountStatement(IJoinedFlatTableDesc intermediateTableDesc, String outputDir) {
        StringBuilder sql = new StringBuilder();
        sql.append("set hive.exec.compress.output=false;\n");
        sql.append("INSERT OVERWRITE DIRECTORY '" + outputDir + "' SELECT count(*) FROM " + intermediateTableDesc.getTableName() + ";\n");
        return sql.toString();
    }

    public static String generateRedistributeFlatTableStatement(IJoinedFlatTableDesc intermediateTableDesc) {
        final String tableName = intermediateTableDesc.getTableName();
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName);

        String redistributeCol = null;
        TblColRef distDcol = intermediateTableDesc.getDistributedBy();
        if (distDcol != null) {
            redistributeCol = colName(distDcol.getCanonicalName());
        }
        appendDistributeStatement(sql, redistributeCol);
        return sql.toString();
    }

}
