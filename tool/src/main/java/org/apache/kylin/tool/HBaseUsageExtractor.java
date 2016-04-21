package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HBaseUsageExtractor extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeMetaExtractor.class);
    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false).withDescription("Specify which cube to extract").create("cube");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(false).withDescription("specify the dest dir to save the related metadata").create("destDir");

    private List<String> htables = Lists.newArrayList();
    private Configuration conf;
    private CubeManager cubeManager;
    private RealizationRegistry realizationRegistry;
    private KylinConfig kylinConfig;
    private ProjectManager projectManager;
    private Options options = null;

    public HBaseUsageExtractor() {
        options = new Options();

        OptionGroup realizationOrProject = new OptionGroup();
        realizationOrProject.addOption(OPTION_CUBE);
        realizationOrProject.addOption(OPTION_PROJECT);
        realizationOrProject.setRequired(true);

        options.addOptionGroup(realizationOrProject);
        options.addOption(OPTION_DEST);

        conf = HBaseConfiguration.create();
    }

    public static void main(String[] args) {
        HBaseUsageExtractor extractor = new HBaseUsageExtractor();
        extractor.execute(args);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    private String getHBaseMasterUrl() {
        String host = conf.get("hbase.master.info.bindAddress");
        String port = conf.get("hbase.master.info.port");
        return "http://" + host + ":" + port + "/";
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String dest = null;
        if (optionsHelper.hasOption(OPTION_DEST)) {
            dest = optionsHelper.getOptionValue(OPTION_DEST);
        }

        if (org.apache.commons.lang3.StringUtils.isEmpty(dest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }

        if (!dest.endsWith("/")) {
            dest = dest + "/";
        }

        kylinConfig = KylinConfig.getInstanceFromEnv();
        cubeManager = CubeManager.getInstance(kylinConfig);
        realizationRegistry = RealizationRegistry.getInstance(kylinConfig);
        projectManager = ProjectManager.getInstance(kylinConfig);

        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            String projectName = optionsHelper.getOptionValue(OPTION_PROJECT);
            ProjectInstance projectInstance = projectManager.getProject(projectName);
            if (projectInstance == null) {
                throw new IllegalArgumentException("Project " + projectName + " does not exist");
            }
            List<RealizationEntry> realizationEntries = projectInstance.getRealizationEntries();
            for (RealizationEntry realizationEntry : realizationEntries) {
                retrieveResourcePath(getRealization(realizationEntry));
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
            IRealization realization;
            if ((realization = cubeManager.getRealization(cubeName)) != null) {
                retrieveResourcePath(realization);
            } else {
                throw new IllegalArgumentException("No cube found with name of " + cubeName);
            }
        }

        extractCommonInfo(dest);
        extractHTables(dest);

        logger.info("Extracted metadata files located at: " + new File(dest).getAbsolutePath());
    }

    private void extractHTables(String dest) throws IOException {
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

    private void extractCommonInfo(String dest) throws IOException {
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

        // hbase hdfs status
        try {
            File hdfsDir = new File(dest, "hdfs");
            FileUtils.forceMkdir(hdfsDir);
            CliCommandExecutor cliCommandExecutor = kylinConfig.getCliCommandExecutor();
            String output = cliCommandExecutor.execute("hadoop fs -ls -R " + conf.get("hbase.rootdir") + "/data/default/KYLIN_*").getSecond();
            FileUtils.writeStringToFile(new File(hdfsDir, "hdfs-files.list"), output);
            output = cliCommandExecutor.execute("hadoop fs -ls -R " + conf.get("hbase.rootdir") + "/data/default/kylin_*").getSecond();
            FileUtils.writeStringToFile(new File(hdfsDir, "hdfs-files.list"), output, true);
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
