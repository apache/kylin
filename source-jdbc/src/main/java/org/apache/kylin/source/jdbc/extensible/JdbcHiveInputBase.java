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

package org.apache.kylin.source.jdbc.extensible;

import org.apache.calcite.sql.SqlDialect;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.util.FlatTableSqlQuoteUtils;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.conv.SqlConverter;
import org.apache.kylin.source.jdbc.sqoop.SqoopCmdStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class JdbcHiveInputBase extends org.apache.kylin.source.jdbc.JdbcHiveInputBase {

    private static final Logger logger = LoggerFactory.getLogger(JdbcHiveInputBase.class);

    public static class JDBCBaseBatchCubingInputSide extends JdbcBaseBatchCubingInputSide {
        private final JdbcConnector dataSource;

        public JDBCBaseBatchCubingInputSide(IJoinedFlatTableDesc flatDesc, JdbcConnector dataSource) {
            super(flatDesc, true);
            this.dataSource = dataSource;
        }

        protected JdbcConnector getDataSource() {
            return dataSource;
        }

        @Override
        protected AbstractExecutable createSqoopToFlatHiveStep(String jobWorkingDir, String cubeName) {
            KylinConfig config = flatDesc.getDataModel().getConfig();
            PartitionDesc partitionDesc = flatDesc.getDataModel().getPartitionDesc();
            String partCol = null;
            boolean enableQuote = dataSource.getSqlConverter().getConfigurer().enableQuote();
            SqlDialect sqlDialect = enableQuote ? dataSource.getSqlConverter().getConfigurer().getSqlDialect() : FlatTableSqlQuoteUtils.NON_QUOTE_DIALECT;
            SqlConverter.IConfigurer iconfigurer = dataSource.getSqlConverter().getConfigurer();

            if (partitionDesc.isPartitioned()) {
                partCol = partitionDesc.getPartitionDateColumn(); //tablename.colname
            }

            String splitTable;
            String splitTableAlias;
            String splitColumn;
            String splitDatabase;
            TblColRef splitColRef = determineSplitColumn();
            splitTable = splitColRef.getTableRef().getTableDesc().getName();
            splitTableAlias = splitColRef.getTableAlias();
            splitDatabase = splitColRef.getColumnDesc().getTable().getDatabase();

            if (enableQuote) {
                splitColumn = sqlDialect.quoteIdentifier(splitColRef.getTableAlias()) + "."
                        + sqlDialect.quoteIdentifier(splitColRef.getName());
                splitDatabase = sqlDialect.quoteIdentifier(splitDatabase);
                splitTable = sqlDialect.quoteIdentifier(splitTable);
                splitTableAlias = sqlDialect.quoteIdentifier(splitTableAlias);
            } else {
                splitColumn = splitColRef.getTableAlias() + "." + splitColRef.getName();
            }

            String selectSql = JoinedFlatTable.generateSelectDataStatement(flatDesc, true, new String[]{partCol}, sqlDialect);
            selectSql = escapeQuotationInSql(dataSource.convertSql(selectSql));

            String hiveTable = flatDesc.getTableName();
            String filedDelimiter = config.getJdbcSourceFieldDelimiter();
            int mapperNum = config.getSqoopMapperNum();

            String bquery;
            bquery = String.format(Locale.ROOT, "SELECT min(%s), max(%s) FROM %s.%s as %s",
                    splitColumn, splitColumn, splitDatabase, splitTable, splitTableAlias);
            bquery = dataSource.convertSql(bquery);

            if (partitionDesc.isPartitioned()) {
                SegmentRange segRange = flatDesc.getSegRange();
                if (segRange != null && !segRange.isInfinite()) {
                    if (partitionDesc.getPartitionDateColumnRef().getTableAlias().equals(splitTableAlias)
                            && (partitionDesc.getPartitionTimeColumnRef() == null || partitionDesc
                                    .getPartitionTimeColumnRef().getTableAlias().equals(splitTableAlias))) {
                        String quotedPartCond = FlatTableSqlQuoteUtils.quoteIdentifierInSqlExpr(flatDesc,
                                partitionDesc.getPartitionConditionBuilder().buildDateRangeCondition(partitionDesc,
                                        flatDesc.getSegment(), segRange, null), sqlDialect);
                        bquery += " WHERE " + quotedPartCond;
                    }
                }
            }

            bquery = escapeQuotationInSql(bquery);
            splitColumn = escapeQuotationInSql(splitColumn);
            String cmd = StringUtils.format(
                    "--connect \"%s\" --driver \"%s\" --username \"%s\" --password \"%s\" --query \"%s AND \\$CONDITIONS\" "
                            + "--target-dir \"%s/%s\" --split-by \"%s\" --boundary-query \"%s\" --null-string '' "
                            + "--fields-terminated-by '%s' --num-mappers %d",
                    dataSource.getJdbcUrl(), dataSource.getJdbcDriver(), dataSource.getJdbcUser(),
                    dataSource.getJdbcPassword(), selectSql, jobWorkingDir, hiveTable, splitColumn, bquery,
                    filedDelimiter, mapperNum);
            if (iconfigurer.getTransactionIsolationLevel() != null) {
                cmd = cmd + " --relaxed-isolation --metadata-transaction-isolation-level "
                        + iconfigurer.getTransactionIsolationLevel();
            }
            logger.debug("sqoop cmd: {}", cmd);

            SqoopCmdStep step = new SqoopCmdStep();
            step.setCmd(cmd);
            step.setName(ExecutableConstants.STEP_NAME_SQOOP_TO_FLAT_HIVE_TABLE);
            return step;
        }
    }
}
