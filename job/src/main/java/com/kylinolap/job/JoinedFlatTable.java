/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.kylinolap.common.util.StringUtil;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.hive.JoinedFlatTableDesc;
import com.kylinolap.job.hadoop.hive.JoinedFlatTableDesc.IntermediateColumnDesc;
import com.kylinolap.job.hadoop.hive.SqlHiveDataTypeMapping;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.DimensionDesc;
import com.kylinolap.metadata.model.cube.JoinDesc;
import com.kylinolap.metadata.model.cube.TblColRef;

/**
 * @author George Song (ysong1)
 * 
 */
public class JoinedFlatTable {

    public static String getTableDir(JoinedFlatTableDesc intermediateTableDesc, String storageDfsDir, String jobUUID) {
        return storageDfsDir + "/" + intermediateTableDesc.getTableName(jobUUID);
    }

    public static String generateCreateTableStatement(JoinedFlatTableDesc intermediateTableDesc, String storageDfsDir, String jobUUID) {
        StringBuilder ddl = new StringBuilder();

        ddl.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + intermediateTableDesc.getTableName(jobUUID) + "\n");

        ddl.append("(" + "\n");
        for (int i = 0; i < intermediateTableDesc.getColumnList().size(); i++) {
            IntermediateColumnDesc col = intermediateTableDesc.getColumnList().get(i);
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getColumnName() + " " + SqlHiveDataTypeMapping.getHiveDataType(col.getDataType()) + "\n");
        }
        ddl.append(")" + "\n");

        ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\177'" + "\n");
        ddl.append("STORED AS SEQUENCEFILE" + "\n");
        ddl.append("LOCATION '" + storageDfsDir + "/" + intermediateTableDesc.getTableName(jobUUID) + "'" + ";");
        // ddl.append("TBLPROPERTIES ('serialization.null.format'='\\\\N')" +
        // ";\n");
        return ddl.toString();
    }

    public static String generateDropTableStatement(JoinedFlatTableDesc intermediateTableDesc, String jobUUID) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("DROP TABLE IF EXISTS " + intermediateTableDesc.getTableName(jobUUID) + ";");
        return ddl.toString();
    }

    public static String generateInsertDataStatement(JoinedFlatTableDesc intermediateTableDesc, String jobUUID, JobEngineConfig engineConfig) throws IOException {
        StringBuilder sql = new StringBuilder();

        File hadoopPropertiesFile = new File(engineConfig.getHadoopJobConfFilePath(intermediateTableDesc.getCubeDesc().getCapacity()));

        if (hadoopPropertiesFile.exists()) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder;
            Document doc;
            try {
                builder = factory.newDocumentBuilder();
                doc = builder.parse(hadoopPropertiesFile);
                NodeList nl = doc.getElementsByTagName("property");
                for (int i = 0; i < nl.getLength(); i++) {
                    String name = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                    String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                    if (name.equals("tmpjars") == false) {
                        sql.append("SET " + name + "=" + value + ";\n");
                    }
                }

            } catch (ParserConfigurationException e) {
                throw new IOException(e);
            } catch (SAXException e) {
                throw new IOException(e);
            }
        }

        // hard coded below mr parameters to enable map-side join
        sql.append("SET hive.exec.compress.output=true;" + "\n");
        sql.append("SET hive.auto.convert.join.noconditionaltask = true;" + "\n");
        sql.append("SET hive.auto.convert.join.noconditionaltask.size = 300000000;" + "\n");
        sql.append("INSERT OVERWRITE TABLE " + intermediateTableDesc.getTableName(jobUUID) + "\n");

        sql.append(generateSelectDataStatement(intermediateTableDesc));
        sql.append(";");
        return sql.toString();
    }

    public static String generateSelectDataStatement(JoinedFlatTableDesc intermediateTableDesc) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + "\n");
        for (int i = 0; i < intermediateTableDesc.getColumnList().size(); i++) {
            IntermediateColumnDesc col = intermediateTableDesc.getColumnList().get(i);
            if (i > 0) {
                sql.append(",");
            }
            sql.append(col.getTableName() + "." + col.getColumnName() + "\n");
        }
        appendJoinStatement(intermediateTableDesc, sql);
        appendWhereStatement(intermediateTableDesc, sql);
        return sql.toString();
    }

    private static void appendJoinStatement(JoinedFlatTableDesc intermediateTableDesc, StringBuilder sql) {
        Set<String> dimTableCache = new HashSet<String>();

        CubeDesc cubeDesc = intermediateTableDesc.getCubeDesc();
        String factTableName = cubeDesc.getFactTable();
        sql.append("FROM " + factTableName + "\n");

        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            JoinDesc join = dim.getJoin();
            if (join != null && join.getType().equals("") == false) {
                String joinType = join.getType().toUpperCase();
                String dimTableName = dim.getTable();
                if (!dimTableCache.contains(dimTableName)) {
                    TblColRef[] pk = join.getPrimaryKeyColumns();
                    TblColRef[] fk = join.getForeignKeyColumns();
                    if (pk.length != fk.length) {
                        throw new RuntimeException("Invalid join condition of dimension " + dim.getName());
                    }
                    sql.append(joinType + " JOIN " + dimTableName + "\n");
                    sql.append("ON ");
                    for (int i = 0; i < pk.length; i++) {
                        if (i > 0) {
                            sql.append(" AND ");
                        }
                        sql.append(factTableName + "." + fk[i].getName() + " = " + dimTableName + "." + pk[i].getName());
                    }
                    sql.append("\n");

                    dimTableCache.add(dimTableName);
                }
            }
        }
    }

    private static void appendWhereStatement(JoinedFlatTableDesc intermediateTableDesc, StringBuilder sql) {
        boolean hasCondition = false;
        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("WHERE");

        CubeDesc cubeDesc = intermediateTableDesc.getCubeDesc();

        if (cubeDesc.getFilterCondition() != null && cubeDesc.getFilterCondition().equals("") == false) {
            whereBuilder.append(" (").append(cubeDesc.getFilterCondition()).append(") ");
            hasCondition = true;
        }

        CubeSegment cubeSegment = intermediateTableDesc.getCubeSegment();

        if (null != cubeSegment) {
            long dateStart = cubeSegment.getDateRangeStart();
            long dateEnd = cubeSegment.getDateRangeEnd();

            if (cubeSegment.getCubeInstance().needMergeImmediatelyAfterBuild(cubeSegment)) {
                dateStart = cubeSegment.getCubeInstance().getDateRange()[1];
            }
            if (!(dateStart == 0 && dateEnd == 0)) {
                String partitionColumnName = cubeDesc.getCubePartitionDesc().getPartitionDateColumn();

                whereBuilder.append(hasCondition ? " AND (" : " (");
                if (dateStart > 0) {
                    whereBuilder.append(partitionColumnName + " >= '" + formatDateTimeInWhereClause(dateStart) + "' ");
                    whereBuilder.append("AND ");
                }
                whereBuilder.append(partitionColumnName + " < '" + formatDateTimeInWhereClause(dateEnd) + "'");
                whereBuilder.append(")\n");
                hasCondition = true;
            }
        }

        if (hasCondition) {
            sql.append(whereBuilder.toString());
        }
    }

    private static String formatDateTimeInWhereClause(long datetime) {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = new Date(datetime);
        String str = f.format(date);
        // note "2014-10-01" >= "2014-10-01 00:00:00" is FALSE
        return StringUtil.dropSuffix(str, " 00:00:00");
    }
}
