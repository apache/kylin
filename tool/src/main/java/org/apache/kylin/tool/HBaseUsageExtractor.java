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

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HBaseUsageExtractor extends AbstractInfoExtractor {

    private static final Logger logger = LoggerFactory.getLogger(CubeMetaExtractor.class);
    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false).withDescription("Specify which cube to extract").create("cube");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    private List<String> htables = Lists.newArrayList();
    private Configuration conf;
    private CubeManager cubeManager;
    private RealizationRegistry realizationRegistry;
    private KylinConfig kylinConfig;
    private ProjectManager projectManager;

    public HBaseUsageExtractor() {
        super();
        packageType = "hbase";

        OptionGroup realizationOrProject = new OptionGroup();
        realizationOrProject.addOption(OPTION_CUBE);
        realizationOrProject.addOption(OPTION_PROJECT);
        realizationOrProject.setRequired(true);

        options.addOptionGroup(realizationOrProject);
        conf = HBaseConfiguration.create();
    }

    public static void main(String[] args) {
        HBaseUsageExtractor extractor = new HBaseUsageExtractor();
        extractor.execute(args);
    }

    private String getHBaseMasterUrl() throws IOException, KeeperException {
        String host = conf.get("hbase.master.info.bindAddress");
        if (host.equals("0.0.0.0")) {
            host = MasterAddressTracker.getMasterAddress(new ZooKeeperWatcher(conf, null, null)).getHostname();
        }

        String port = conf.get("hbase.master.info.port");
        return "http://" + host + ":" + port + "/";
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        cubeManager = CubeManager.getInstance(kylinConfig);
        realizationRegistry = RealizationRegistry.getInstance(kylinConfig);
        projectManager = ProjectManager.getInstance(kylinConfig);

        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            String projectNames = optionsHelper.getOptionValue(OPTION_PROJECT);
            for (String projectName: projectNames.split(",")) {
                ProjectInstance projectInstance = projectManager.getProject(projectName);
                if (projectInstance == null) {
                    throw new IllegalArgumentException("Project " + projectName + " does not exist");
                }
                List<RealizationEntry> realizationEntries = projectInstance.getRealizationEntries();
                for (RealizationEntry realizationEntry : realizationEntries) {
                    retrieveResourcePath(getRealization(realizationEntry));
                }
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeNames = optionsHelper.getOptionValue(OPTION_CUBE);
            for (String cubeName : cubeNames.split(",")) {
                IRealization realization = cubeManager.getRealization(cubeName);
                if (realization != null) {
                    retrieveResourcePath(realization);
                } else {
                    throw new IllegalArgumentException("No cube found with name of " + cubeName);
                }
            }
        }

        extractCommonInfo(exportDir);
        extractHTables(exportDir);
    }

    private void extractHTables(File dest) throws IOException {
        logger.info("These htables are going to be extracted:");
        for (String htable : htables) {
            logger.info(htable + "(required)");
        }

        File tableDir = new File(dest, "table");
        FileUtils.forceMkdir(tableDir);

        for (String htable : htables) {
            try {
                URL srcUrl = new URL(getHBaseMasterUrl() + "table.jsp?name=" + htable);
                File destFile = new File(tableDir, htable + ".html");
                FileUtils.copyURLToFile(srcUrl, destFile);
            } catch (Exception e) {
                logger.warn("HTable " + htable + "info fetch failed: ", e);
            }
        }
    }

    private void extractCommonInfo(File dest) throws IOException {
        logger.info("The hbase master info/conf are going to be extracted...");

        // hbase master page
        try {
            File masterDir = new File(dest, "master");
            FileUtils.forceMkdir(masterDir);
            URL srcMasterUrl = new URL(getHBaseMasterUrl() + "master-status");
            File masterDestFile = new File(masterDir, "master-status.html");
            FileUtils.copyURLToFile(srcMasterUrl, masterDestFile);
        } catch (Exception e) {
            logger.warn("HBase Master status fetch failed: ", e);
        }

        // hbase conf
        try {
            File confDir = new File(dest, "conf");
            FileUtils.forceMkdir(confDir);
            URL srcConfUrl = new URL(getHBaseMasterUrl() + "conf");
            File destConfFile = new File(confDir, "hbase-conf.xml");
            FileUtils.copyURLToFile(srcConfUrl, destConfFile);
        } catch (Exception e) {
            logger.warn("HBase conf fetch failed: ", e);
        }

        // hbase jmx
        try {
            File jmxDir = new File(dest, "jmx");
            FileUtils.forceMkdir(jmxDir);
            URL srcJmxUrl = new URL(getHBaseMasterUrl() + "jmx");
            File jmxDestFile = new File(jmxDir, "jmx.html");
            FileUtils.copyURLToFile(srcJmxUrl, jmxDestFile);
        } catch (Exception e) {
            logger.warn("HBase JMX fetch failed: ", e);
        }

        // hbase hdfs status
        try {
            File hdfsDir = new File(dest, "hdfs");
            FileUtils.forceMkdir(hdfsDir);
            CliCommandExecutor cliCommandExecutor = kylinConfig.getCliCommandExecutor();
            String output = cliCommandExecutor.execute("hadoop fs -ls -R " + conf.get("hbase.rootdir") + "/data/default/KYLIN_*").getSecond();
            FileUtils.writeStringToFile(new File(hdfsDir, "hdfs-files.list"), output, Charset.defaultCharset());
            output = cliCommandExecutor.execute("hadoop fs -ls -R " + conf.get("hbase.rootdir") + "/data/default/kylin_*").getSecond();
            FileUtils.writeStringToFile(new File(hdfsDir, "hdfs-files.list"), output, Charset.defaultCharset(), true);
        } catch (Exception e) {
            logger.warn("HBase hdfs status fetch failed: ", e);
        }
    }

    private IRealization getRealization(RealizationEntry realizationEntry) {
        return realizationRegistry.getRealization(realizationEntry.getType(), realizationEntry.getRealization());
    }

    private void retrieveResourcePath(IRealization realization) {

        logger.info("Deal with realization {} of type {}", realization.getName(), realization.getType());

        if (realization instanceof CubeInstance) {
            CubeInstance cube = (CubeInstance) realization;
            for (CubeSegment segment : cube.getSegments()) {
                addHTable(segment.getStorageLocationIdentifier());
            }
        } else {
            logger.warn("Unknown realization type: " + realization.getType());
        }
    }

    private void addHTable(String record) {
        logger.info("adding required resource {}", record);
        htables.add(record);
    }
}
