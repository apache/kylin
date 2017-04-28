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

package org.apache.kylin.engine.mr.common;

/**
 * @author George Song (ysong1)
 *
 */

import static org.apache.hadoop.util.StringUtils.formatTime;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("static-access")
public abstract class AbstractHadoopJob extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHadoopJob.class);

    protected static final Option OPTION_JOB_NAME = OptionBuilder.withArgName(BatchConstants.ARG_JOB_NAME).hasArg().isRequired(true).withDescription("Job name. For example, Kylin_Cuboid_Builder-clsfd_v2_Step_22-D)").create(BatchConstants.ARG_JOB_NAME);
    protected static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg().isRequired(true).withDescription("Cube name. For exmaple, flat_item_cube").create(BatchConstants.ARG_CUBE_NAME);
    protected static final Option OPTION_CUBING_JOB_ID = OptionBuilder.withArgName(BatchConstants.ARG_CUBING_JOB_ID).hasArg().isRequired(false).withDescription("ID of cubing job executable").create(BatchConstants.ARG_CUBING_JOB_ID);
    //    @Deprecated
    protected static final Option OPTION_SEGMENT_NAME = OptionBuilder.withArgName(BatchConstants.ARG_SEGMENT_NAME).hasArg().isRequired(true).withDescription("Cube segment name").create(BatchConstants.ARG_SEGMENT_NAME);
    protected static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName(BatchConstants.ARG_SEGMENT_ID).hasArg().isRequired(true).withDescription("Cube segment id").create(BatchConstants.ARG_SEGMENT_ID);
    protected static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg().isRequired(true).withDescription("Input path").create(BatchConstants.ARG_INPUT);
    protected static final Option OPTION_INPUT_FORMAT = OptionBuilder.withArgName(BatchConstants.ARG_INPUT_FORMAT).hasArg().isRequired(false).withDescription("Input format").create(BatchConstants.ARG_INPUT_FORMAT);
    protected static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg().isRequired(true).withDescription("Output path").create(BatchConstants.ARG_OUTPUT);
    protected static final Option OPTION_NCUBOID_LEVEL = OptionBuilder.withArgName(BatchConstants.ARG_LEVEL).hasArg().isRequired(true).withDescription("N-Cuboid build level, e.g. 1, 2, 3...").create(BatchConstants.ARG_LEVEL);
    protected static final Option OPTION_PARTITION_FILE_PATH = OptionBuilder.withArgName(BatchConstants.ARG_PARTITION).hasArg().isRequired(true).withDescription("Partition file path.").create(BatchConstants.ARG_PARTITION);
    protected static final Option OPTION_HTABLE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_HTABLE_NAME).hasArg().isRequired(true).withDescription("HTable name").create(BatchConstants.ARG_HTABLE_NAME);

    protected static final Option OPTION_STATISTICS_ENABLED = OptionBuilder.withArgName(BatchConstants.ARG_STATS_ENABLED).hasArg().isRequired(false).withDescription("Statistics enabled").create(BatchConstants.ARG_STATS_ENABLED);
    protected static final Option OPTION_STATISTICS_OUTPUT = OptionBuilder.withArgName(BatchConstants.ARG_STATS_OUTPUT).hasArg().isRequired(false).withDescription("Statistics output").create(BatchConstants.ARG_STATS_OUTPUT);
    protected static final Option OPTION_STATISTICS_SAMPLING_PERCENT = OptionBuilder.withArgName(BatchConstants.ARG_STATS_SAMPLING_PERCENT).hasArg().isRequired(false).withDescription("Statistics sampling percentage").create(BatchConstants.ARG_STATS_SAMPLING_PERCENT);

    private static final String MAP_REDUCE_CLASSPATH = "mapreduce.application.classpath";

    protected static void runJob(Tool job, String[] args) {
        try {
            int exitCode = ToolRunner.run(job, args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(5);
        }
    }

    // ============================================================================

    protected String name;
    protected boolean isAsync = false;
    protected OptionsHelper optionsHelper = new OptionsHelper();

    protected Job job;

    public AbstractHadoopJob() {
        super(HadoopUtil.getCurrentConfiguration());
    }

    protected void parseOptions(Options options, String[] args) throws ParseException {
        optionsHelper.parseOptions(options, args);
    }

    public void printUsage(Options options) {
        optionsHelper.printUsage(getClass().getSimpleName(), options);
    }

    public Option[] getOptions() {
        return optionsHelper.getOptions();
    }

    public String getOptionsAsString() {
        return optionsHelper.getOptionsAsString();
    }

    protected String getOptionValue(Option option) {
        return optionsHelper.getOptionValue(option);
    }

    protected boolean hasOption(Option option) {
        return optionsHelper.hasOption(option);
    }

    protected int waitForCompletion(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        int retVal = 0;
        long start = System.nanoTime();
        if (isAsync) {
            job.submit();
        } else {
            job.waitForCompletion(true);
            retVal = job.isSuccessful() ? 0 : 1;
            logger.debug("Job '" + job.getJobName() + "' finished " + (job.isSuccessful() ? "successfully in " : "with failures.  Time taken ") + formatTime((System.nanoTime() - start) / 1000000L));
        }
        return retVal;
    }

    protected void setJobClasspath(Job job, KylinConfig kylinConf) {
        String jarPath = kylinConf.getKylinJobJarPath();
        File jarFile = new File(jarPath);
        if (jarFile.exists()) {
            job.setJar(jarPath);
            logger.info("append job jar: " + jarPath);
        } else {
            job.setJarByClass(this.getClass());
        }

        String kylinHiveDependency = System.getProperty("kylin.hive.dependency");
        String kylinHBaseDependency = System.getProperty("kylin.hbase.dependency");
        String kylinKafkaDependency = System.getProperty("kylin.kafka.dependency");
        logger.info("append kylin.hbase.dependency: " + kylinHBaseDependency + " to " + MAP_REDUCE_CLASSPATH);

        Configuration jobConf = job.getConfiguration();
        String classpath = jobConf.get(MAP_REDUCE_CLASSPATH);
        if (classpath == null || classpath.length() == 0) {
            logger.info("Didn't find " + MAP_REDUCE_CLASSPATH + " in job configuration, will run 'mapred classpath' to get the default value.");
            classpath = getDefaultMapRedClasspath();
            logger.info("The default mapred classpath is: " + classpath);
        }

        if (kylinHBaseDependency != null) {
            // yarn classpath is comma separated
            kylinHBaseDependency = kylinHBaseDependency.replace(":", ",");
            classpath = classpath + "," + kylinHBaseDependency;
        }

        jobConf.set(MAP_REDUCE_CLASSPATH, classpath);
        logger.info("Hadoop job classpath is: " + job.getConfiguration().get(MAP_REDUCE_CLASSPATH));

        /*
         *  set extra dependencies as tmpjars & tmpfiles if configured
         */
        StringBuilder kylinDependency = new StringBuilder();

        // for hive dependencies
        if (kylinHiveDependency != null) {
            // yarn classpath is comma separated
            kylinHiveDependency = kylinHiveDependency.replace(":", ",");

            logger.info("Hive Dependencies Before Filtered: " + kylinHiveDependency);
            String filteredHive = filterKylinHiveDependency(kylinHiveDependency, kylinConf);
            logger.info("Hive Dependencies After Filtered: " + filteredHive);

            StringUtil.appendWithSeparator(kylinDependency, filteredHive);
        } else {

            logger.info("No hive dependency jars set in the environment, will find them from classpath:");

            try {
                String hiveExecJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.hadoop.hive.ql.Driver"));

                StringUtil.appendWithSeparator(kylinDependency, hiveExecJarPath);
                logger.info("hive-exec jar file: " + hiveExecJarPath);

                String hiveHCatJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.hive.hcatalog.mapreduce.HCatInputFormat"));
                StringUtil.appendWithSeparator(kylinDependency, hiveHCatJarPath);
                logger.info("hive-catalog jar file: " + hiveHCatJarPath);

                String hiveMetaStoreJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.hadoop.hive.metastore.api.Table"));
                StringUtil.appendWithSeparator(kylinDependency, hiveMetaStoreJarPath);
                logger.info("hive-metastore jar file: " + hiveMetaStoreJarPath);
            } catch (ClassNotFoundException e) {
                logger.error("Cannot found hive dependency jars: " + e);
            }
        }

        // for kafka dependencies
        if (kylinKafkaDependency != null) {
            kylinKafkaDependency = kylinKafkaDependency.replace(":", ",");
            logger.info("Kafka Dependencies: " + kylinKafkaDependency);
            StringUtil.appendWithSeparator(kylinDependency, kylinKafkaDependency);
        } else {
            logger.info("No Kafka dependency jar set in the environment, will find them from classpath:");
            try {
                String kafkaClientJarPath = ClassUtil.findContainingJar(Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer"));
                StringUtil.appendWithSeparator(kylinDependency, kafkaClientJarPath);
                logger.info("kafka jar file: " + kafkaClientJarPath);

            } catch (ClassNotFoundException e) {
                logger.debug("Not found kafka client jar from classpath, it is optional for normal build: " + e);
            }
        }

        // for KylinJobMRLibDir
        String mrLibDir = kylinConf.getKylinJobMRLibDir();
        StringUtil.appendWithSeparator(kylinDependency, mrLibDir);

        setJobTmpJarsAndFiles(job, kylinDependency.toString());

        overrideJobConfig(job.getConfiguration(), kylinConf.getMRConfigOverride());
    }

    private void overrideJobConfig(Configuration jobConf, Map<String, String> override) {
        for (Entry<String, String> entry : override.entrySet()) {
            jobConf.set(entry.getKey(), entry.getValue());
        }
    }

    private String filterKylinHiveDependency(String kylinHiveDependency, KylinConfig config) {
        if (StringUtils.isBlank(kylinHiveDependency))
            return "";

        StringBuilder jarList = new StringBuilder();

        Pattern hivePattern = Pattern.compile(config.getHiveDependencyFilterList());
        Matcher matcher = hivePattern.matcher(kylinHiveDependency);

        while (matcher.find()) {
            if (jarList.length() > 0)
                jarList.append(",");
            jarList.append(matcher.group());
        }

        return jarList.toString();
    }

    private void setJobTmpJarsAndFiles(Job job, String kylinDependency) {
        if (StringUtils.isBlank(kylinDependency))
            return;

        String[] fNameList = kylinDependency.split(",");

        try {
            Configuration jobConf = job.getConfiguration();
            FileSystem localfs = FileSystem.getLocal(jobConf);
            FileSystem hdfs = HadoopUtil.getWorkingFileSystem(jobConf);

            StringBuilder jarList = new StringBuilder();
            StringBuilder fileList = new StringBuilder();

            for (String fileName : fNameList) {
                Path p = new Path(fileName);
                if (p.isAbsolute() == false) {
                    logger.warn("The directory of kylin dependency '" + fileName + "' is not absolute, skip");
                    continue;
                }
                FileSystem fs;
                if (exists(hdfs, p)) {
                    fs = hdfs;
                } else if (exists(localfs, p)) {
                    fs = localfs;
                } else {
                    logger.warn("The directory of kylin dependency '" + fileName + "' does not exist, skip");
                    continue;
                }
                
                if (fs.getFileStatus(p).isDirectory()) {
                    appendTmpDir(job, fs, p, jarList, fileList);
                    continue;
                }

                StringBuilder list = (p.getName().endsWith(".jar")) ? jarList : fileList;
                if (list.length() > 0)
                    list.append(",");
                list.append(fs.getFileStatus(p).getPath());
            }

            appendTmpFiles(fileList.toString(), jobConf);
            appendTmpJars(jarList.toString(), jobConf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendTmpDir(Job job, FileSystem fs, Path tmpDir, StringBuilder jarList, StringBuilder fileList) {
        try {
            FileStatus[] fList = fs.listStatus(tmpDir);

            for (FileStatus file : fList) {
                Path p = file.getPath();
                if (fs.getFileStatus(p).isDirectory()) {
                    appendTmpDir(job, fs, p, jarList, fileList);
                    continue;
                }

                StringBuilder list = (p.getName().endsWith(".jar")) ? jarList : fileList;
                if (list.length() > 0)
                    list.append(",");
                list.append(fs.getFileStatus(p).getPath().toString());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendTmpJars(String jarList, Configuration conf) {
        if (StringUtils.isBlank(jarList))
            return;

        String tmpJars = conf.get("tmpjars", null);
        if (tmpJars == null) {
            tmpJars = jarList;
        } else {
            tmpJars += "," + jarList;
        }
        conf.set("tmpjars", tmpJars);
        logger.info("Job 'tmpjars' updated -- " + tmpJars);
    }

    private void appendTmpFiles(String fileList, Configuration conf) {
        if (StringUtils.isBlank(fileList))
            return;

        String tmpFiles = conf.get("tmpfiles", null);
        if (tmpFiles == null) {
            tmpFiles = fileList;
        } else {
            tmpFiles += "," + fileList;
        }
        conf.set("tmpfiles", tmpFiles);
        logger.info("Job 'tmpfiles' updated -- " + tmpFiles);
    }

    private String getDefaultMapRedClasspath() {

        String classpath = "";
        try {
            CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
            String output = executor.execute("mapred classpath").getSecond();
            classpath = output.trim().replace(':', ',');
        } catch (IOException e) {
            logger.error("Failed to run: 'mapred classpath'.", e);
        }

        return classpath;
    }

    private static boolean exists(FileSystem fs, Path p) throws IOException {
        try {
            return fs.exists(p);
        } catch (IllegalArgumentException ex) {
            // can happen when FS mismatch
            return false;
        }
    }

    public static int addInputDirs(String input, Job job) throws IOException {
        int folderNum = addInputDirs(StringSplitter.split(input, ","), job);
        logger.info("Number of added folders:" + folderNum);
        return folderNum;
    }

    public static int addInputDirs(String[] inputs, Job job) throws IOException {
        int ret = 0;//return number of added folders
        for (String inp : inputs) {
            inp = inp.trim();
            if (inp.endsWith("/*")) {
                inp = inp.substring(0, inp.length() - 2);
                FileSystem fs = HadoopUtil.getWorkingFileSystem(job.getConfiguration());
                Path path = new Path(inp);

                if (!exists(fs, path)) {
                    logger.warn("Path not exist:" + path.toString());
                    continue;
                }

                FileStatus[] fileStatuses = fs.listStatus(path);
                boolean hasDir = false;
                for (FileStatus stat : fileStatuses) {
                    if (stat.isDirectory() && !stat.getPath().getName().startsWith("_")) {
                        hasDir = true;
                        ret += addInputDirs(new String[] { stat.getPath().toString() }, job);
                    }
                }
                if (fileStatuses.length > 0 && !hasDir) {
                    ret += addInputDirs(new String[] { path.toString() }, job);
                }
            } else {
                logger.debug("Add input " + inp);
                FileInputFormat.addInputPath(job, new Path(inp));
                ret++;
            }
        }
        return ret;
    }

    public static KylinConfig loadKylinPropsAndMetadata() throws IOException {
        File metaDir = new File("meta");
        if (!metaDir.getAbsolutePath().equals(System.getProperty(KylinConfig.KYLIN_CONF))) {
            System.setProperty(KylinConfig.KYLIN_CONF, metaDir.getAbsolutePath());
            logger.info("The absolute path for meta dir is " + metaDir.getAbsolutePath());
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            kylinConfig.setMetadataUrl(metaDir.getAbsolutePath());
            return kylinConfig;
        } else {
            return KylinConfig.getInstanceFromEnv();
        }
    }

    protected void attachTableMetadata(TableDesc table, Configuration conf) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.add(table.getResourcePath());
        dumpKylinPropsAndMetadata(dumpList, KylinConfig.getInstanceFromEnv(), conf);
    }

    protected void attachCubeMetadata(CubeInstance cube, Configuration conf) throws IOException {
        dumpKylinPropsAndMetadata(collectCubeMetadata(cube), cube.getConfig(), conf);
    }

    protected void attachCubeMetadataWithDict(CubeInstance cube, Configuration conf) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.addAll(collectCubeMetadata(cube));
        for (CubeSegment segment : cube.getSegments()) {
            dumpList.addAll(segment.getDictionaryPaths());
        }
        dumpKylinPropsAndMetadata(dumpList, cube.getConfig(), conf);
    }

    protected void attachSegmentMetadataWithDict(CubeSegment segment, Configuration conf) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.addAll(collectCubeMetadata(segment.getCubeInstance()));
        dumpList.addAll(segment.getDictionaryPaths());
        dumpKylinPropsAndMetadata(dumpList, segment.getConfig(), conf);
    }

    private Set<String> collectCubeMetadata(CubeInstance cube) {
        // cube, model_desc, cube_desc, table
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getModel().getResourcePath());
        dumpList.add(cube.getDescriptor().getResourcePath());

        for (TableRef tableRef : cube.getDescriptor().getModel().getAllTables()) {
            TableDesc table = tableRef.getTableDesc();
            dumpList.add(table.getResourcePath());
            dumpList.addAll(SourceFactory.getMRDependentResources(table));
        }

        return dumpList;
    }

    protected void dumpKylinPropsAndMetadata(Set<String> dumpList, KylinConfig kylinConfig, Configuration conf) throws IOException {
        File tmp = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmp); // we need a directory, so delete the file first

        File metaDir = new File(tmp, "meta");
        metaDir.mkdirs();

        // write kylin.properties
        File kylinPropsFile = new File(metaDir, "kylin.properties");
        kylinConfig.writeProperties(kylinPropsFile);

        // write resources
        dumpResources(kylinConfig, metaDir, dumpList);

        // hadoop distributed cache
        String hdfsMetaDir = OptionsHelper.convertToFileURL(metaDir.getAbsolutePath());
        if (hdfsMetaDir.startsWith("/")) // note Path on windows is like "d:/../..."
            hdfsMetaDir = "file://" + hdfsMetaDir;
        else
            hdfsMetaDir = "file:///" + hdfsMetaDir;
        logger.info("HDFS meta dir is: " + hdfsMetaDir);

        appendTmpFiles(hdfsMetaDir, conf);
    }

    protected void cleanupTempConfFile(Configuration conf) {
        String tempMetaFileString = conf.get("tmpfiles");
        logger.info("tempMetaFileString is : " + tempMetaFileString);
        if (tempMetaFileString != null) {
            if (tempMetaFileString.startsWith("file://")) {
                tempMetaFileString = tempMetaFileString.substring("file://".length());
                File tempMetaFile = new File(tempMetaFileString);
                if (tempMetaFile.exists()) {
                    try {
                        FileUtils.forceDelete(tempMetaFile.getParentFile());

                    } catch (IOException e) {
                        logger.warn("error when deleting " + tempMetaFile, e);
                    }
                } else {
                    logger.info("" + tempMetaFileString + " does not exist");
                }
            } else {
                logger.info("tempMetaFileString is not starting with file:// :" + tempMetaFileString);
            }
        }
    }

    private void dumpResources(KylinConfig kylinConfig, File metaDir, Set<String> dumpList) throws IOException {
        long startTime = System.currentTimeMillis();

        ResourceStore from = ResourceStore.getStore(kylinConfig);
        KylinConfig localConfig = KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath());
        ResourceStore to = ResourceStore.getStore(localConfig);
        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            to.putResource(path, res.inputStream, res.timestamp);
            res.inputStream.close();
        }

        logger.debug("Dump resources to {} took {} ms", metaDir, System.currentTimeMillis() - startTime);
    }

    protected void deletePath(Configuration conf, Path path) throws IOException {
        HadoopUtil.deletePath(conf, path);
    }

    protected double getTotalMapInputMB() throws ClassNotFoundException, IOException, InterruptedException, JobException {
        if (job == null) {
            throw new JobException("Job is null");
        }

        long mapInputBytes = 0;
        InputFormat<?, ?> input = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
        for (InputSplit split : input.getSplits(job)) {
            mapInputBytes += split.getLength();
        }
        if (mapInputBytes == 0) {
            throw new IllegalArgumentException("Map input splits are 0 bytes, something is wrong!");
        }
        double totalMapInputMB = (double) mapInputBytes / 1024 / 1024;
        return totalMapInputMB;
    }

    protected int getMapInputSplitCount() throws ClassNotFoundException, JobException, IOException, InterruptedException {
        if (job == null) {
            throw new JobException("Job is null");
        }
        InputFormat<?, ?> input = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
        return input.getSplits(job).size();
    }

    public void kill() throws JobException {
        if (job != null) {
            try {
                job.killJob();
            } catch (IOException e) {
                throw new JobException(e);
            }
        }
    }

    public Map<String, String> getInfo() throws JobException {
        if (job != null) {
            Map<String, String> status = new HashMap<String, String>();
            if (null != job.getJobID()) {
                status.put(JobInstance.MR_JOB_ID, job.getJobID().toString());
            }
            if (null != job.getTrackingURL()) {
                status.put(JobInstance.YARN_APP_URL, job.getTrackingURL().toString());
            }

            return status;
        } else {
            throw new JobException("Job is null");
        }
    }

    public Counters getCounters() throws JobException {
        if (job != null) {
            try {
                return job.getCounters();
            } catch (IOException e) {
                throw new JobException(e);
            }
        } else {
            throw new JobException("Job is null");
        }
    }

    public void setAsync(boolean isAsync) {
        this.isAsync = isAsync;
    }

    public Job getJob() {
        return this.job;
    }

    // tells MapReduceExecutable to skip this job
    public boolean isSkipped() {
        return false;
    }

}
