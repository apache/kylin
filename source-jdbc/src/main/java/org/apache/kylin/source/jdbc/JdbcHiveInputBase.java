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
package org.apache.kylin.source.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SourceDialect;
import org.apache.kylin.common.util.SourceConfigurationUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.util.FlatTableSqlQuoteUtils;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.hive.DBConnConf;
import org.apache.kylin.source.hive.HiveInputBase;
import org.apache.kylin.source.jdbc.metadata.IJdbcMetadata;
import org.apache.kylin.source.jdbc.metadata.JdbcMetadataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class JdbcHiveInputBase extends HiveInputBase {
    private static final Logger logger = LoggerFactory.getLogger(JdbcHiveInputBase.class);
    private static final String MR_OVERRIDE_QUEUE_KEY = "mapreduce.job.queuename";
    private static final String DEFAULT_QUEUE = "default";

    public static class JdbcBaseBatchCubingInputSide extends BaseBatchCubingInputSide {
        private IJdbcMetadata jdbcMetadataDialect;
        private DBConnConf dbconf;
        private SourceDialect dialect;
        private final Map<String, String> metaMap = new TreeMap<>();

        public JdbcBaseBatchCubingInputSide(IJoinedFlatTableDesc flatDesc, boolean skipCacheMeta) {
            super(flatDesc);
            if (!skipCacheMeta) {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                String connectionUrl = config.getJdbcSourceConnectionUrl();
                String driverClass = config.getJdbcSourceDriver();
                String jdbcUser = config.getJdbcSourceUser();
                String jdbcPass = config.getJdbcSourcePass();
                dbconf = new DBConnConf(driverClass, connectionUrl, jdbcUser, jdbcPass);
                dialect = SourceDialect.getDialect(config.getJdbcSourceDialect());
                jdbcMetadataDialect = JdbcMetadataFactory.getJdbcMetadata(dialect, dbconf);
                calCachedJdbcMeta(metaMap, dbconf, jdbcMetadataDialect);
                if (logger.isTraceEnabled()) {
                    StringBuilder dumpInfo = new StringBuilder();
                    metaMap.forEach((k, v) -> dumpInfo.append("CachedMetadata: ").append(k).append(" => ").append(v)
                            .append(System.lineSeparator()));
                    logger.trace(dumpInfo.toString());
                }
            }
        }

        /**
         * Fetch and cache metadata from JDBC API, which will help to resolve
         * case-sensitive problem of sql identifier
         *
         * @param metadataMap a Map which mapping upper case identifier to real/original identifier
         */
        public static void calCachedJdbcMeta(Map<String, String> metadataMap, DBConnConf dbconf,
                IJdbcMetadata jdbcMetadataDialect) {
            try (Connection connection = SqlUtil.getConnection(dbconf)) {
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                for (String database : jdbcMetadataDialect.listDatabases()) {
                    metadataMap.put(database.toUpperCase(Locale.ROOT), database);
                    ResultSet tableRs = jdbcMetadataDialect.getTable(databaseMetaData, database, null);
                    while (tableRs.next()) {
                        String tableName = tableRs.getString("TABLE_NAME");
                        ResultSet colRs = jdbcMetadataDialect.listColumns(databaseMetaData, database, tableName);
                        while (colRs.next()) {
                            String colName = colRs.getString("COLUMN_NAME");
                            colName = database + "." + tableName + "." + colName;
                            metadataMap.put(colName.toUpperCase(Locale.ROOT), colName);
                        }
                        colRs.close();
                        tableName = database + "." + tableName;
                        metadataMap.put(tableName.toUpperCase(Locale.ROOT), tableName);
                    }
                    tableRs.close();
                }
            } catch (IllegalStateException e) {
                if (SqlUtil.DRIVER_MISS.equalsIgnoreCase(e.getMessage())) {
                    logger.warn("Ignore JDBC Driver Missing in yarn node.", e);
                } else {
                    throw e;
                }
            } catch (Exception e) {
                throw new IllegalStateException("Error when connect to JDBC source " + dbconf.getUrl(), e);
            }
        }

        protected KylinConfig getConfig() {
            return flatDesc.getDataModel().getConfig();
        }

        @Override
        protected void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            jobFlow.addTask(createSqoopToFlatHiveStep(jobWorkingDir, cubeName));
            jobFlow.addTask(createFlatHiveTableFromFiles(hiveInitStatements, jobWorkingDir));
        }

        @Override
        protected void addStepPhase1_DoMaterializeLookupTable(DefaultChainedExecutable jobFlow) {
            // skip
        }

        private AbstractExecutable createFlatHiveTableFromFiles(String hiveInitStatements, String jobWorkingDir) {
            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
            String filedDelimiter = getConfig().getJdbcSourceFieldDelimiter();
            // Sqoop does not support exporting SEQUENSEFILE to Hive now SQOOP-869
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, jobWorkingDir,
                    "TEXTFILE", filedDelimiter);

            HiveCmdStep step = new HiveCmdStep();
            step.setCmd(hiveInitStatements + dropTableHql + createTableHql);
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            return step;
        }

        /**
         * Choose a better split-by column for sqoop. The strategy is:
         * 1. Prefer ClusteredBy column
         * 2. Prefer DistributedBy column
         * 3. Prefer Partition date column
         * 4. Prefer Higher cardinality column
         * 5. Prefer numeric column
         * 6. Pick a column at first glance
         * @return A column reference <code>TblColRef</code>for sqoop split-by
         */
        protected TblColRef determineSplitColumn() {
            if (null != flatDesc.getClusterBy()) {
                return flatDesc.getClusterBy();
            }
            if (null != flatDesc.getDistributedBy()) {
                return flatDesc.getDistributedBy();
            }
            PartitionDesc partitionDesc = flatDesc.getDataModel().getPartitionDesc();
            if (partitionDesc.isPartitioned()) {
                return partitionDesc.getPartitionDateColumnRef();
            }
            TblColRef splitColumn = null;
            TableMetadataManager tblManager = TableMetadataManager.getInstance(getConfig());
            long maxCardinality = 0;
            for (TableRef tableRef : flatDesc.getDataModel().getAllTables()) {
                TableExtDesc tableExtDesc = tblManager.getTableExt(tableRef.getTableDesc());
                List<TableExtDesc.ColumnStats> columnStatses = tableExtDesc.getColumnStats();
                if (!columnStatses.isEmpty()) {
                    for (TblColRef colRef : tableRef.getColumns()) {
                        long cardinality = columnStatses.get(colRef.getColumnDesc().getZeroBasedIndex())
                                .getCardinality();
                        splitColumn = cardinality > maxCardinality ? colRef : splitColumn;
                    }
                }
            }
            if (null == splitColumn) {
                for (TblColRef colRef : flatDesc.getAllColumns()) {
                    if (colRef.getType().isIntegerFamily()) {
                        return colRef;
                    }
                }
                splitColumn = flatDesc.getAllColumns().get(0);
            }

            return splitColumn;
        }

        private String getSqoopJobQueueName(KylinConfig config) {
            Map<String, String> mrConfigOverride = config.getMRConfigOverride();
            if (mrConfigOverride.containsKey(MR_OVERRIDE_QUEUE_KEY)) {
                return mrConfigOverride.get(MR_OVERRIDE_QUEUE_KEY);
            }
            return DEFAULT_QUEUE;
        }

        protected AbstractExecutable createSqoopToFlatHiveStep(String jobWorkingDir, String cubeName) {
            KylinConfig config = getConfig();
            PartitionDesc partitionDesc = flatDesc.getDataModel().getPartitionDesc();
            String partCol = null;

            if (partitionDesc.isPartitioned()) {
                partCol = partitionDesc.getPartitionDateColumn();//tablename.colname
            }

            String splitTableAlias;
            String splitColumn;
            String splitDatabase;
            TblColRef splitColRef = determineSplitColumn();
            splitTableAlias = splitColRef.getTableAlias();

            splitColumn = getColumnIdentityQuoted(splitColRef, jdbcMetadataDialect, metaMap, true);
            splitDatabase = splitColRef.getColumnDesc().getTable().getDatabase();

            String selectSql = generateSelectDataStatementRDBMS(flatDesc, true, new String[] { partCol },
                    jdbcMetadataDialect, metaMap);
            selectSql = escapeQuotationInSql(selectSql);

            String hiveTable = flatDesc.getTableName();
            String connectionUrl = config.getJdbcSourceConnectionUrl();
            String driverClass = config.getJdbcSourceDriver();
            String jdbcUser = config.getJdbcSourceUser();
            String jdbcPass = config.getJdbcSourcePass();
            String sqoopHome = config.getSqoopHome();
            String sqoopNullString = config.getSqoopNullString();
            String sqoopNullNonString = config.getSqoopNullNonString();
            String filedDelimiter = config.getJdbcSourceFieldDelimiter();
            int mapperNum = config.getSqoopMapperNum();

            String bquery = String.format(Locale.ROOT, "SELECT min(%s), max(%s) FROM %s.%s ", splitColumn,
                    splitColumn, getSchemaQuoted(metaMap, splitDatabase, jdbcMetadataDialect, true),
                    getTableIdentityQuoted(splitColRef.getTableRef(), metaMap, jdbcMetadataDialect, true));
            if (partitionDesc.isPartitioned()) {
                SegmentRange segRange = flatDesc.getSegRange();
                if (segRange != null && !segRange.isInfinite()) {
                    if (partitionDesc.getPartitionDateColumnRef().getTableAlias().equals(splitTableAlias)
                            && (partitionDesc.getPartitionTimeColumnRef() == null || partitionDesc
                                    .getPartitionTimeColumnRef().getTableAlias().equals(splitTableAlias))) {

                        String quotedPartCond = partitionDesc.getPartitionConditionBuilder().buildDateRangeCondition(
                                partitionDesc, flatDesc.getSegment(), segRange,
                                col -> getTableColumnIdentityQuoted(col, jdbcMetadataDialect, metaMap, true));
                        bquery += " WHERE " + quotedPartCond;
                    }
                }
            }
            bquery = escapeQuotationInSql(bquery);

            // escape ` in cmd
            splitColumn = escapeQuotationInSql(splitColumn);

            String cmd = String.format(Locale.ROOT, "%s/bin/sqoop import" + generateSqoopConfigArgString()
                    + "--connect \"%s\" --driver %s --username %s --password \"%s\" --query \"%s AND \\$CONDITIONS\" "
                    + "--target-dir %s/%s --split-by %s --boundary-query \"%s\" --null-string '%s' "
                    + "--null-non-string '%s' --fields-terminated-by '%s' --num-mappers %d", sqoopHome, connectionUrl,
                    driverClass, jdbcUser, jdbcPass, selectSql, jobWorkingDir, hiveTable, splitColumn, bquery,
                    sqoopNullString, sqoopNullNonString, filedDelimiter, mapperNum);
            logger.debug("sqoop cmd : {}", cmd);
            CmdStep step = new CmdStep();
            step.setCmd(cmd);
            step.setName(ExecutableConstants.STEP_NAME_SQOOP_TO_FLAT_HIVE_TABLE);
            return step;
        }

        protected String generateSqoopConfigArgString() {
            KylinConfig kylinConfig = getConfig();
            Map<String, String> config = Maps.newHashMap();
            config.put(MR_OVERRIDE_QUEUE_KEY, getSqoopJobQueueName(kylinConfig)); // override job queue from mapreduce config
            config.putAll(SourceConfigurationUtil.loadSqoopConfiguration());
            config.putAll(kylinConfig.getSqoopConfigOverride());

            StringBuilder args = new StringBuilder(" -Dorg.apache.sqoop.splitter.allow_text_splitter=true ");
            for (Map.Entry<String, String> entry : config.entrySet()) {
                args.append(" -D" + entry.getKey() + "=" + entry.getValue() + " ");
            }
            return args.toString();
        }
    }

    protected static String escapeQuotationInSql(String sqlExpr) {
        sqlExpr = sqlExpr.replaceAll("\"", "\\\\\"");
        sqlExpr = sqlExpr.replaceAll("`", "\\\\`");
        return sqlExpr;
    }

    private static String generateSelectDataStatementRDBMS(IJoinedFlatTableDesc flatDesc, boolean singleLine,
            String[] skipAs, IJdbcMetadata metadata, Map<String, String> metaMap) {
        SourceDialect dialect = metadata.getDialect();
        final String sep = singleLine ? " " : "\n";

        final List<String> skipAsList = (skipAs == null) ? new ArrayList<>() : Arrays.asList(skipAs);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT");
        sql.append(sep);

        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                sql.append(",");
            }
            String colTotalName = String.format(Locale.ROOT, "%s.%s", col.getTableRef().getTableName(), col.getName());
            if (skipAsList.contains(colTotalName)) {
                sql.append(getTableColumnIdentityQuoted(col, metadata, metaMap, true)).append(sep);
            } else {
                sql.append(getTableColumnIdentityQuoted(col, metadata, metaMap, true)).append(" as ")
                        .append(quoteIdentifier(JoinedFlatTable.colName(col), dialect)).append(sep);
            }
        }
        appendJoinStatement(flatDesc, sql, singleLine, metadata, metaMap);
        appendWhereStatement(flatDesc, sql, singleLine, metadata, metaMap);
        return sql.toString();
    }

    private static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine,
            IJdbcMetadata metadata, Map<String, String> metaMap) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = new HashSet<>();

        DataModelDesc model = flatDesc.getDataModel();
        sql.append(" FROM ")
                .append(getSchemaQuoted(metaMap,
                        flatDesc.getDataModel().getRootFactTable().getTableDesc().getDatabase(), metadata, true))
                .append(".")
                .append(getTableIdentityQuoted(flatDesc.getDataModel().getRootFactTable(), metaMap, metadata, true));

        sql.append(" ");
        sql.append((getTableIdentityQuoted(flatDesc.getDataModel().getRootFactTable(), metaMap, metadata, true)))
                .append(sep);

        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            if (join != null && !join.getType().equals("")) {
                TableRef dimTable = lookupDesc.getTableRef();
                if (!dimTableCache.contains(dimTable)) {
                    TblColRef[] pk = join.getPrimaryKeyColumns();
                    TblColRef[] fk = join.getForeignKeyColumns();
                    if (pk.length != fk.length) {
                        throw new RuntimeException("Invalid join condition of lookup table:" + lookupDesc);
                    }
                    String joinType = join.getType().toUpperCase(Locale.ROOT);

                    sql.append(joinType).append(" JOIN ")
                            .append(getSchemaQuoted(metaMap, dimTable.getTableDesc().getDatabase(), metadata, true))
                            .append(".").append(getTableIdentityQuoted(dimTable, metaMap, metadata, true));

                    sql.append(" ");
                    sql.append(getTableIdentityQuoted(dimTable, metaMap, metadata, true)).append(sep);
                    sql.append("ON ");
                    for (int i = 0; i < pk.length; i++) {
                        if (i > 0) {
                            sql.append(" AND ");
                        }
                        sql.append(getTableColumnIdentityQuoted(fk[i], metadata, metaMap, true)).append(" = ")
                                .append(getTableColumnIdentityQuoted(pk[i], metadata, metaMap, true));
                    }
                    sql.append(sep);
                    dimTableCache.add(dimTable);
                }
            }
        }
    }

    private static void appendWhereStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine,
            IJdbcMetadata metadata, Map<String, String> metaMap) {
        final String sep = singleLine ? " " : "\n";

        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("WHERE 1=1");

        DataModelDesc model = flatDesc.getDataModel();
        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            whereBuilder.append(" AND (").append(model.getFilterCondition()).append(") ");
        }

        if (flatDesc.getSegment() != null) {
            PartitionDesc partDesc = model.getPartitionDesc();
            if (partDesc != null && partDesc.getPartitionDateColumn() != null) {
                SegmentRange segRange = flatDesc.getSegRange();

                if (segRange != null && !segRange.isInfinite()) {
                    whereBuilder.append(" AND (");
                    whereBuilder.append(partDesc.getPartitionConditionBuilder().buildDateRangeCondition(partDesc,
                            flatDesc.getSegment(), segRange,
                            col -> getTableColumnIdentityQuoted(col, metadata, metaMap, true)));
                    whereBuilder.append(")");
                    whereBuilder.append(sep);
                }
            }
        }
        sql.append(whereBuilder.toString());
    }

    /**
     * @return {TABLE_NAME}.{COLUMN_NAME}
     */
    private static String getTableColumnIdentityQuoted(TblColRef col, IJdbcMetadata metadata,
            Map<String, String> metaMap, boolean needQuote) {
        String tblName = getTableIdentityQuoted(col.getTableRef(), metaMap, metadata, needQuote);
        String colName = getColumnIdentityQuoted(col, metadata, metaMap, needQuote);
        return tblName + "." + colName;
    }

    /**
     * @return {SCHEMA_NAME}
     */
    static String getSchemaQuoted(Map<String, String> metaMap, String database, IJdbcMetadata metadata,
            boolean needQuote) {
        String databaseName = fetchValue(database, null, null, metaMap);
        if (needQuote) {
            return quoteIdentifier(databaseName, metadata.getDialect());
        } else {
            return databaseName;
        }
    }

    /**
     * @return {TABLE_NAME}
     */
    static String getTableIdentityQuoted(TableRef tableRef, Map<String, String> metaMap, IJdbcMetadata metadata,
            boolean needQuote) {
        String value = fetchValue(tableRef.getTableDesc().getDatabase(), tableRef.getTableDesc().getName(), null,
                metaMap);
        String[] res = value.split("\\.");
        value = res[res.length - 1];
        if (needQuote) {
            return quoteIdentifier(value, metadata.getDialect());
        } else {
            return value;
        }
    }

    /**
     * @return {TABLE_NAME}
     */
    static String getTableIdentityQuoted(String database, String table, Map<String, String> metaMap,
            IJdbcMetadata metadata, boolean needQuote) {
        String value = fetchValue(database, table, null, metaMap);
        String[] res = value.split("\\.");
        value = res[res.length - 1];
        if (needQuote) {
            return quoteIdentifier(value, metadata.getDialect());
        } else {
            return value;
        }
    }

    /**
     * @return {COLUMN_NAME}
     */
    private static String getColumnIdentityQuoted(TblColRef tblColRef, IJdbcMetadata metadata,
            Map<String, String> metaMap, boolean needQuote) {
        String value = fetchValue(tblColRef.getTableRef().getTableDesc().getDatabase(),
                tblColRef.getTableRef().getTableDesc().getName(), tblColRef.getName(), metaMap);
        String[] res = value.split("\\.");
        value = res[res.length - 1];
        if (needQuote) {
            return quoteIdentifier(value, metadata.getDialect());
        } else {
            return value;
        }
    }

    /**
     * @param identifier something looks like tableA.columnB
     */
    static String quoteIdentifier(String identifier, SourceDialect dialect) {
        if (KylinConfig.getInstanceFromEnv().enableHiveDdlQuote()) {
            String[] identifierArray = identifier.split("\\.");
            String quoted = "";
            for (int i = 0; i < identifierArray.length; i++) {
                identifierArray[i] = FlatTableSqlQuoteUtils.quoteIdentifier(dialect, identifierArray[i]);
            }
            quoted = String.join(".", identifierArray);
            return quoted;
        } else {
            return identifier;
        }
    }

    static String fetchValue(String database, String table, String column, Map<String, String> metadataMap) {
        String key;
        if (table == null && column == null) {
            key = database;
        } else if (column == null) {
            key = database + "." + table;
        } else {
            key = database + "." + table + "." + column;
        }
        String val = metadataMap.get(key.toUpperCase(Locale.ROOT));
        if (val == null) {
            logger.warn("Not find for {} from metadata cache.", key);
            return key;
        } else {
            return val;
        }
    }
}
