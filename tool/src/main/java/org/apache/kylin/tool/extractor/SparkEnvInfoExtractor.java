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
 *
 */

package org.apache.kylin.tool.extractor;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SparkEnvInfoExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(SparkEnvInfoExtractor.class);

    public SparkEnvInfoExtractor(){
        super();
        packageType = "spark";
    }

    private File getSparkConfDir() {
        String sparkHome = KylinConfig.getSparkHome();
        File sparkHomeDir = new File(sparkHome);
        Preconditions.checkArgument(sparkHomeDir.exists(), "Your SPARK_HOME does not exist.");
        return new File(sparkHomeDir, "conf");
    }

    private File getHadoopConfDir() {
        String hadoopConf = System.getenv("HADOOP_CONF_DIR");
        // maybe add a user defined env setting in kylin.properties
        Preconditions.checkNotNull(hadoopConf, "Cannot find HADOOP_CONF_DIR in the environment.");
        File hadoopConfDir = new File(hadoopConf);
        Preconditions.checkArgument(hadoopConfDir.exists(), "Your HADOOP_CONF_DIR does not exist: " + hadoopConf);
        return hadoopConfDir;
    }

    private void extractConfDir(File from, File to) {
        File[] confFiles = from.listFiles();
        if (confFiles != null) {
            for (File confFile : confFiles) {
                if (!confFile.getName().endsWith(".template")) {
                    addFile(confFile, to);
                }
            }
        }
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        StringBuilder envStrBuilder = new StringBuilder();

        // extract spark configurations
        try {
            File sparkHome = getSparkConfDir();
            extractConfDir(sparkHome, new File(exportDir, "spark-conf"));
            envStrBuilder.append("SPARK_HOME=").append(sparkHome.getAbsolutePath()).append("\n");
        } catch (Exception e) {
            logger.error("Failed to extract spark conf: error={}", e.getMessage());
        }

        // extract hadoop configurations for spark
        try {
            File hadoopConf = getHadoopConfDir();
            extractConfDir(hadoopConf, new File(exportDir, "hadoop-conf"));
            envStrBuilder.append("HADOOP_CONF_DIR=").append(hadoopConf.getAbsolutePath()).append("\n");
        } catch (Exception e) {
            logger.error("Failed to extract hadoop conf: error={}", e.getMessage());
        }

        // extract spark env variables
        FileUtils.write(new File(exportDir, "env"), envStrBuilder.toString());
    }
}
