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

package org.apache.kylin.job.deployment;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.apache.kylin.job.tools.LZOSupportnessChecker;

/**
 * <p/>
 * This class is assumed to be run by
 * "hbase org.apache.hadoop.util.RunJar kylin-job-0.5.7-SNAPSHOT-job.jar org.apache.kylin.job.deployment.HadoopConfigPrinter "
 * in the shell, so that hbase and hadoop related environment variables will be
 * visible to this class.
 */
public class HbaseConfigPrinterCLI {
    public static void main(String[] args) throws IOException {

        if (args[0].equalsIgnoreCase("printconfig"))
            printConfigs(args[1]);

        if (args[0].equalsIgnoreCase("printenv"))
            printAllEnv();

        if (args[0].equalsIgnoreCase("printprop"))
            printAllProperties();

        if (args[0].equalsIgnoreCase("printhbaseconf"))
            printHbaseConf();
    }

    private static void printConfigs(String targetFile) throws IOException {

        File output = new File(targetFile);
        if (output.exists() && output.isDirectory()) {
            throw new IllegalStateException("The output file: " + targetFile + " is a directory");
        }

        StringBuilder sb = new StringBuilder();

        sb.append("export KYLIN_LZO_SUPPORTED=" + ConfigLoader.LZO_INFO_LOADER.loadValue() + "\n");
        sb.append("export KYLIN_LD_LIBRARY_PATH=" + ConfigLoader.LD_LIBRARY_PATH_LOADER.loadValue() + "\n");
        sb.append("export KYLIN_HBASE_CLASSPATH=" + ConfigLoader.HBASE_CLASSPATH_LOADER.loadValue() + "\n");
        sb.append("export KYLIN_HBASE_CONF_PATH=" + ConfigLoader.HBASE_CONF_FOLDER_LOADER.loadValue() + "\n");
        sb.append("export KYLIN_ZOOKEEPER_QUORUM=" + ConfigLoader.ZOOKEEP_QUORUM_LOADER.loadValue() + "\n");
        sb.append("export KYLIN_ZOOKEEPER_CLIENT_PORT=" + ConfigLoader.ZOOKEEPER_CLIENT_PORT_LOADER.loadValue() + "\n");
        sb.append("export KYLIN_ZOOKEEPER_ZNODE_PARENT=" + ConfigLoader.ZOOKEEPER_ZNODE_PARENT_LOADER.loadValue() + "\n");

        FileUtils.writeStringToFile(output, sb.toString());
    }

    private static void printHbaseConf() {
        Configuration conf = HBaseConfiguration.create();
        for (Map.Entry<String, String> entry : conf) {
            System.out.println("Key: " + entry.getKey());
            System.out.println("Value: " + entry.getValue());
            System.out.println();
        }
    }

    private static void printAllProperties() {
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            System.out.println("Key: " + entry.getKey());
            System.out.println("Value: " + entry.getValue());
            System.out.println();
        }
    }

    private static void printAllEnv() {
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            System.out.println("Key: " + entry.getKey());
            System.out.println("Value: " + entry.getValue());
            System.out.println();
        }
    }

    enum ConfigLoader {

        LZO_INFO_LOADER {
            @Override
            public String loadValue() {
                return LZOSupportnessChecker.getSupportness() ? "true" : "false";
            }
        },

        ZOOKEEP_QUORUM_LOADER {
            @Override
            public String loadValue() {
                Configuration conf = HBaseConfiguration.create();
                return conf.get(HConstants.ZOOKEEPER_QUORUM);
            }
        },

        ZOOKEEPER_ZNODE_PARENT_LOADER {
            @Override
            public String loadValue() {
                Configuration conf = HBaseConfiguration.create();
                return conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            }
        },

        ZOOKEEPER_CLIENT_PORT_LOADER {
            @Override
            public String loadValue() {
                Configuration conf = HBaseConfiguration.create();
                return conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);

            }
        },

        LD_LIBRARY_PATH_LOADER {
            @Override
            public String loadValue() {
                return System.getenv("LD_LIBRARY_PATH");
            }
        },

        HBASE_CLASSPATH_LOADER {
            @Override
            public String loadValue() {
                return System.getenv("CLASSPATH");
            }
        },

        HBASE_CONF_FOLDER_LOADER {
            @Override
            public String loadValue() {
                String output = HBASE_CLASSPATH_LOADER.loadValue();
                String[] paths = output.split(":");
                StringBuilder sb = new StringBuilder();

                for (String path : paths) {
                    path = path.trim();
                    File f = new File(path);
                    if (StringUtils.containsIgnoreCase(path, "conf") && f.exists() && f.isDirectory() && f.getName().equalsIgnoreCase("conf")) {
                        sb.append(":" + path);
                    }
                }
                return sb.toString();
            }
        };

        public abstract String loadValue();
    }

}
