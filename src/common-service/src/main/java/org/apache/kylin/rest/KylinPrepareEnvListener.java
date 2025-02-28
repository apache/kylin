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
package org.apache.kylin.rest;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.tool.kerberos.DelegationTokenManager;
import org.apache.kylin.source.jdbc.H2Database;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinPrepareEnvListener implements EnvironmentPostProcessor, Ordered {

    @Override
    public int getOrder() {
        return ConfigDataEnvironmentPostProcessor.ORDER + 1010;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication application) {

        if (env.getPropertySources().contains("bootstrap")) {
            return;
        }

        if (env.getActiveProfiles().length == 0) {
            env.addActiveProfile("dev");
        }

        if (env.acceptsProfiles(Profiles.of("sandbox"))) {
            if (env.acceptsProfiles(Profiles.of("docker"))) {
                setSandboxEnvs("../../dev-support/contributor/sandbox/conf");
            } else {
                setSandboxEnvs("../../../kylin/src/examples/test_case_data/sandbox");
            }
        } else if (env.acceptsProfiles(Profiles.of("dev"))) {
            if (env.getSystemEnvironment().containsKey(KylinConfig.KYLIN_CONF))
                ClassUtil.addClasspath(env.getSystemEnvironment().get(KylinConfig.KYLIN_CONF).toString());
            if (env.getProperty("dev.dont-load-test-metadata") == null
                    && !StringUtils.equals("true", env.getProperty("dev.diag-meta")))
                setLocalEnvs();
        }
        // enable CC check
        Unsafe.setProperty("needCheckCC", "true");
        val config = KylinConfig.getInstanceFromEnv();
        if (config.isCalciteCompatibleWithMsSqlPlusEnabled()) {
            Unsafe.setProperty("calcite.compatible-with-mssql-plus", KylinConfigBase.TRUE);
        } else {
            Unsafe.setProperty("calcite.compatible-with-mssql-plus", KylinConfigBase.FALSE);
        }
        Unsafe.setProperty("calcite.bindable.cache.maxSize", Integer.toString(config.getCalciteBindableCacheSize()));
        Unsafe.setProperty("calcite.bindable.cache.concurrencyLevel",
                Integer.toString(config.getCalciteBindableCacheConcurrencyLevel()));

        TimeZoneUtils.setDefaultTimeZone(config);
        DelegationTokenManager delegationTokenManager = new DelegationTokenManager();
        delegationTokenManager.start();
        env.addActiveProfile(config.getSecurityProfile());

        // add extra hive class paths.
        val extraClassPath = config.getHiveMetastoreExtraClassPath();
        if (StringUtils.isNotEmpty(extraClassPath)) {
            ClassUtil.addToClasspath(extraClassPath, Thread.currentThread().getContextClassLoader());
        }
    }

    private static void setSandboxEnvs(String sandboxEnvPath) {
        File dir1 = new File(sandboxEnvPath);
        ClassUtil.addClasspath(dir1.getAbsolutePath());
        Unsafe.setProperty(KylinConfig.KYLIN_CONF, dir1.getAbsolutePath());

        Unsafe.setProperty("kylin.hadoop.conf.dir", sandboxEnvPath);
        Unsafe.setProperty("hdp.version", "current");

    }

    private static void setLocalEnvs() {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        File localMetadata = new File(tempMetadataDir);

        // pass checkHadoopHome
        Unsafe.setProperty("hadoop.home.dir", localMetadata.getAbsolutePath() + "/working-dir");
        Unsafe.setProperty("spark.local", "true");

        // enable push down
        Unsafe.setProperty("kylin.query.pushdown-enabled", "true");
        Unsafe.setProperty("kylin.query.pushdown.runner-class-name",
                "org.apache.kylin.query.pushdown.PushDownRunnerJdbcImpl");

        // set h2 configuration
        Unsafe.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        Unsafe.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        Unsafe.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        Unsafe.setProperty("kylin.query.pushdown.jdbc.password", "");

        // Load H2 Tables (inner join) for pushdown to rdbms in local debug mode
        try {
            String username = System.getProperty("kylin.query.pushdown.jdbc.username");
            String password = System.getProperty("kylin.query.pushdown.jdbc.password");
            Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", username,
                    password);
            H2Database h2DB = new H2Database(h2Connection, KylinConfig.getInstanceFromEnv(), "default");
            h2DB.loadAllTables();
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        }
    }
}
