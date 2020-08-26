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

package org.apache.kylin.rest.init;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.SparderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.io.File;
import java.nio.file.Paths;

/**
 * Created by zhangzc on 8/26/20.
 */
public class InitialSparkerContext implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(InitialSparkerContext.class);

    @Override
    public void afterPropertiesSet() throws Exception {
        runInitialSparder();
    }

    private void runInitialSparder() {
        logger.info("Spark is starting.....");
        SparderContext.init();
        final String kylinHome = StringUtils.defaultIfBlank(KylinConfig.getKylinHome(), "./");
        final File appidFile = Paths.get(kylinHome, "sparkappid").toFile();
        String appid = null;
        try {
            appid = SparderContext.getSparkSession().sparkContext().applicationId();
            FileUtils.writeStringToFile(appidFile, appid);
            logger.info("Spark application id is {}", appid);
        } catch (Exception e) {
            logger.error("Failed to generate spark application id[{}] file",
                    StringUtils.defaultString(appid), e);
        }
    }
}
