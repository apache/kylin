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
package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.tool.util.ScreenPrintUtil.printlnGreen;
import static org.apache.kylin.tool.util.ScreenPrintUtil.printlnRed;
import static org.apache.kylin.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.persistence.metadata.JdbcEpochStore;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.tool.util.MetadataUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class CreateTableEpochCLI extends ExecutableApplication {

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    public static void main(String[] args) {
        CreateTableEpochCLI createTableEpochCLI = new CreateTableEpochCLI();
        try {
            createTableEpochCLI.execute(args);
        } catch (Exception e) {
            log.error("Failed to exec CreateTableEpochCLI", e);
            systemExitWhenMainThread(1);
        }

        log.info("Upgrade table epoch finished!");
        systemExitWhenMainThread(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_EXEC);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        KylinConfig systemKylinConfig = KylinConfig.getInstanceFromEnv();
        StorageURL url = systemKylinConfig.getMetadataUrl();
        String tableName = url.getIdentifier() + JdbcEpochStore.EPOCH_TABLE_NAME;

        DataSource dataSource = MetadataUtil.getDataSource(systemKylinConfig);

        boolean tableExist = false;
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            printlnGreen("found epoch table already exists.");
            tableExist = true;
        } else {
            printlnGreen("found epoch table doesn't exists.");
        }

        if (optionsHelper.hasOption(OPTION_EXEC)) {
            if (!tableExist) {
                printlnGreen("start to create epoch table.");

                Properties properties = JdbcUtil.getProperties((BasicDataSource) dataSource);
                String createTableStmt = JdbcEpochStore
                        .getEpochSql(properties.getProperty(JdbcEpochStore.CREATE_EPOCH_TABLE), tableName);
                try {
                    MetadataUtil.createTableIfNotExist((org.apache.commons.dbcp2.BasicDataSource) dataSource, tableName,
                            createTableStmt);
                } catch (Exception e) {
                    printlnRed("Failed to create epoch table.");
                    throw e;
                }
            }
            printlnGreen("epoch table upgrade succeeded.");
        }
    }
}
