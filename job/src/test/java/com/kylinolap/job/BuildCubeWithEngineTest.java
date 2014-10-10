/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.common.util.SSHClient;
import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.dataGen.FactTableGenerator;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.InvalidJobInstanceException;
import com.kylinolap.job.hadoop.hive.SqlHiveDataTypeMapping;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;

/**
 * @author ysong1
 */
public class BuildCubeWithEngineTest extends HBaseMetadataTestCase {

	private static final Logger logger = LoggerFactory
			.getLogger(BuildCubeWithEngineTest.class);

	protected static final String TABLE_CAL_DT = "test_cal_dt";
	protected static final String TABLE_CATEGORY_GROUPINGS = "test_category_groupings";
	protected static final String TABLE_KYLIN_FACT = "test_kylin_fact";
	protected static final String TABLE_SELLER_TYPE_DIM = "test_seller_type_dim";
	protected static final String TABLE_SITES = "test_sites";

	String[] TABLE_NAMES = new String[] { TABLE_CAL_DT,
			TABLE_CATEGORY_GROUPINGS, TABLE_KYLIN_FACT, TABLE_SELLER_TYPE_DIM,
			TABLE_SITES };

	protected JobManager jobManager;
	protected JobEngineConfig engineConfig;
	protected String jobJarName;
	protected String coprocessorJarName;

	public void scpFilesToHadoopCli(String[] files, String remoteDir)
			throws Exception {
		for (String file : files) {
			File f = new File(file);
			String filename = f.getName();
			execCommand("mkdir -p " + remoteDir);
			execCommand("rm -f " + remoteDir + "/" + filename);
			execCopy(file, remoteDir);
			execCommand("chmod 755 " + remoteDir + "/" + filename);
		}
	}

	public void removeHdfsDir(SSHClient hadoopCli, String hdfsDir)
			throws Exception {
		String cmd = "hadoop fs -rm -f -r " + hdfsDir;
		assertEquals(0, hadoopCli.execCommand(cmd));
	}

	public void createHdfsDir(SSHClient hadoopCli, String hdfsDir)
			throws Exception {
		String cmd = "hadoop fs -mkdir -p " + hdfsDir;
		assertEquals(0, hadoopCli.execCommand(cmd));
		assertEquals(0,
				hadoopCli.execCommand("hadoop fs -chmod 777 " + hdfsDir));
	}

	public String getHadoopCliWorkingDir() {
		return KylinConfig.getInstanceFromEnv().getCliWorkingDir();
	}

	public String getHadoopCliHostname() {
		return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv()
				.getRemoteHadoopCliHostname() : null;
	}

	public String getHadoopCliUsername() {
		return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv()
				.getRemoteHadoopCliUsername() : null;
	}

	public String getHadoopCliPassword() {
		return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv()
				.getRemoteHadoopCliPassword() : null;
	}

	public boolean getHadoopCliAsRemote() {
		return KylinConfig.getInstanceFromEnv().getRunAsRemoteCommand();
	}

	public void retrieveJarName() throws FileNotFoundException, IOException,
			XmlPullParserException {
		MavenXpp3Reader pomReader = new MavenXpp3Reader();
		Model model = pomReader.read(new FileReader("../pom.xml"));
		String version = model.getVersion();

		this.jobJarName = "kylin-job-" + version + "-job.jar";
		this.coprocessorJarName = "kylin-storage-" + version
				+ "-coprocessor.jar";
	}

	public void scpFilesToHdfs(SSHClient hadoopCli, String[] localFiles,
			String hdfsDir) throws Exception {
		String remoteTempDir = "/tmp/";

		List<String> nameList = new ArrayList<String>();
		for (int i = 0; i < localFiles.length; i++) {
			File file = new File(localFiles[i]);
			String filename = file.getName();
			nameList.add(filename);
		}
		for (String f : localFiles) {
			hadoopCli.scpFileToRemote(f, remoteTempDir);
		}
		for (String f : nameList) {
			hadoopCli.execCommand("hadoop fs -put -f " + remoteTempDir + f
					+ " " + hdfsDir + "/" + f);
			hadoopCli.execCommand("hadoop fs -chmod 777 " + hdfsDir + "/" + f);
		}
	}

	public void execHiveCommand(String hql) throws Exception {
		String hiveCmd = "hive -e \"" + hql + "\"";
		execCommand(hiveCmd);
	}

	public String getTextFromFile(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");
		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}
		reader.close();
		return stringBuilder.toString();
	}

	public String generateLoadDataHql(String tableName) {
		return "LOAD DATA LOCAL INPATH '" + this.getHadoopCliWorkingDir() + "/"
				+ tableName.toUpperCase() + ".csv' OVERWRITE INTO TABLE "
				+ tableName.toUpperCase();
	}

	public String generateCreateTableHql(TableDesc tableDesc) {
		StringBuilder ddl = new StringBuilder();

		ddl.append("DROP TABLE IF EXISTS " + tableDesc.getName() + ";\n");
		ddl.append("CREATE TABLE " + tableDesc.getName() + "\n");
		ddl.append("(" + "\n");

		for (int i = 0; i < tableDesc.getColumns().length; i++) {
			ColumnDesc col = tableDesc.getColumns()[i];
			if (i > 0) {
				ddl.append(",");
			}
			ddl.append(col.getName()
					+ " "
					+ SqlHiveDataTypeMapping.getHiveDataType((col.getDatatype()))
					+ "\n");
		}

		ddl.append(")" + "\n");
		ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + "\n");
		ddl.append("STORED AS TEXTFILE;");

		return ddl.toString();
	}

	public void execCommand(String cmd) {
		int exitCode = 0;
		CommandExecutor cmdExecutor = new CommandExecutor(cmd,
				new VerboseConsumer());
		cmdExecutor.setRunAtRemote(getHadoopCliHostname(),
				getHadoopCliUsername(), getHadoopCliPassword());
		try {
			exitCode = cmdExecutor.execute(true);
		} catch (IOException e) {
			logger.error("Failed to execute command", e);
		}
		assertEquals(0, exitCode);
	}

	public void execCopy(String localFile, String destDir) {
		int exitCode = 0;
		CopyExecutor cpExecutor = new CopyExecutor(localFile, destDir,
				new VerboseConsumer());
		cpExecutor.setRunAtRemote(getHadoopCliHostname(),
				getHadoopCliUsername(), getHadoopCliPassword());
		try {
			exitCode = cpExecutor.execute(true);
		} catch (IOException e) {
			logger.error("Failed to execute command", e);
		}
		assertEquals(0, exitCode);
	}

	public class VerboseConsumer implements CliOutputConsumer {
		@Override
		public void consume(String message) {
			logger.debug(message);
		}

		@Override
		public void log(String message) {
			consume(message);
		}
	}

	private void cleanUp() throws Exception {
		this.execCommand("rm -rf " + this.getHadoopCliWorkingDir());
	}

	private void deployJarToHadoopCli() throws Exception {
		scpFilesToHadoopCli(new String[] { ".." + File.separator + "job"
				+ File.separator + "target" + File.separator + jobJarName },
				this.getHadoopCliWorkingDir());
	}

	private void deployJarToLocalDir() throws IOException {
		File targetFile = new File(KylinConfig.getInstanceFromEnv()
				.getCoprocessorLocalJar());
		File parent = targetFile.getParentFile();
		if (!parent.exists() && !parent.mkdirs()) {
			throw new IllegalStateException("Couldn't create dir: " + parent);
		}

		FileUtils.copyFile(new File(".." + File.separator + "storage"
				+ File.separator + "target" + File.separator
				+ coprocessorJarName), targetFile);
	}

	private void deployConfigFile() throws Exception {
		scpFilesToHadoopCli(new String[] {
				".." + File.separator + "examples" + File.separator
						+ "test_case_data" + File.separator
						+ "kylin.properties",
				".." + File.separator + "examples" + File.separator
						+ "test_case_data" + File.separator
						+ "hadoop_job_conf.xml" }, "/etc/kylin");
	}

	private void deployTestData() throws Exception {

		MetadataManager metaMgr = MetadataManager.getInstance(this
				.getTestConfig());

		// scp data files, use the data from hbase, instead of local files
		File temp = File.createTempFile("temp", ".csv");
		temp.createNewFile();
		for (String tablename : TABLE_NAMES) {
			tablename = tablename.toUpperCase();

			File localBufferFile = new File(temp.getParent() + File.separator
					+ tablename + ".csv");
			localBufferFile.createNewFile();

			InputStream hbaseDataStream = metaMgr.getStore().getResource(
					"/data/" + tablename + ".csv");
			FileOutputStream localFileStream = new FileOutputStream(
					localBufferFile);
			IOUtils.copy(hbaseDataStream, localFileStream);

			hbaseDataStream.close();
			localFileStream.close();

			this.scpFilesToHadoopCli(
					new String[] { localBufferFile.getPath() },
					this.getHadoopCliWorkingDir());

			localBufferFile.delete();
		}
		temp.delete();

		// create hive tables
		this.execHiveCommand(this.generateCreateTableHql(metaMgr
				.getTableDesc(TABLE_CAL_DT.toUpperCase())));
		this.execHiveCommand(this.generateCreateTableHql(metaMgr
				.getTableDesc(TABLE_CATEGORY_GROUPINGS.toUpperCase())));
		this.execHiveCommand(this.generateCreateTableHql(metaMgr
				.getTableDesc(TABLE_KYLIN_FACT.toUpperCase())));
		this.execHiveCommand(this.generateCreateTableHql(metaMgr
				.getTableDesc(TABLE_SELLER_TYPE_DIM.toUpperCase())));
		this.execHiveCommand(this.generateCreateTableHql(metaMgr
				.getTableDesc(TABLE_SITES.toUpperCase())));

		// load data to hive tables
		// LOAD DATA LOCAL INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
		this.execHiveCommand(this.generateLoadDataHql(TABLE_CAL_DT));
		this.execHiveCommand(this.generateLoadDataHql(TABLE_CATEGORY_GROUPINGS));
		this.execHiveCommand(this.generateLoadDataHql(TABLE_KYLIN_FACT));
		this.execHiveCommand(this.generateLoadDataHql(TABLE_SELLER_TYPE_DIM));
		this.execHiveCommand(this.generateLoadDataHql(TABLE_SITES));
	}

	protected void initEnv(boolean deployConfig) throws Exception {
		// create log dir
		this.execCommand("mkdir -p /tmp/kylin/logs");
		retrieveJarName();
		cleanUp();

		// install metadata to hbase
		installMetadataToHBase();

		// update cube desc signature.
		for (CubeInstance cube : CubeManager.getInstance(this.getTestConfig())
				.listAllCubes()) {
			cube.getDescriptor().setSignature(
					cube.getDescriptor().calculateSignature());
			CubeManager.getInstance(this.getTestConfig()).updateCube(cube);
		}

		deployJarToHadoopCli();
		deployJarToLocalDir();

		if (deployConfig)
			deployConfigFile();
	}

	protected void prepareTestData(String joinType) throws Exception {
		if (joinType.equalsIgnoreCase("inner")) {
			// put data to cli
			FactTableGenerator.generate("test_kylin_cube_with_slr_empty",
					"10000", "1", null, "inner");
		} else if (joinType.equalsIgnoreCase("left")) {
			FactTableGenerator.generate(
					"test_kylin_cube_with_slr_left_join_empty", "10000", "0.6",
					null, "left");
		} else {
			throw new Exception("Unsupported join type : " + joinType);
		}

		deployTestData();
	}

	@Before
	public void before() throws Exception {
		ClasspathUtil.addClasspath(new File(
				"../examples/test_case_data/hadoop-site").getAbsolutePath());
		this.createTestMetadata();

		initEnv(true);

		engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
		jobManager = new JobManager("Build_Test_Cube_Engine", engineConfig);
		jobManager.deleteAllJobs();
	}

	@After
	public void after() throws IOException {
		// jobManager.deleteAllJobs();
	}

	@Test
	public void testCubes() throws Exception {

		// start job schedule engine
		jobManager.startJobEngine(10);

		// keep this order.
		testLeftJoinCube();
		testInnerJoinCube();

		jobManager.stopJobEngine();
	}

	/**
	 * For cube test_kylin_cube_with_slr_empty, we will create 2 segments For
	 * cube test_kylin_cube_without_slr_empty, since it doesn't support
	 * incremental build, we will create only 1 segment (full build)
	 * 
	 * @throws Exception
	 */
	private void testInnerJoinCube() throws Exception {
		this.prepareTestData("inner");// default settings;

		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		f.setTimeZone(TimeZone.getTimeZone("GMT"));
		long dateStart;
		long dateEnd;

		ArrayList<String> jobs = new ArrayList<String>();

		// this cube's start date is 0, end date is 20501112000000
		dateStart = 0;
		dateEnd = f.parse("2013-01-01").getTime();
		jobs.addAll(this.submitJob("test_kylin_cube_with_slr_empty", dateStart,
				dateEnd, CubeBuildTypeEnum.BUILD));

		// this cube doesn't support incremental build, always do full build
		jobs.addAll(this.submitJob("test_kylin_cube_without_slr_empty", 0, 0,
				CubeBuildTypeEnum.BUILD));

		waitCubeBuilt(jobs);

		// then submit a incremental job, start date is 20130101000000, end date
		// is 20220101000000
		jobs.clear();
		dateStart = f.parse("2013-01-01").getTime();
		dateEnd = f.parse("2022-01-01").getTime();
		jobs.addAll(this.submitJob("test_kylin_cube_with_slr_empty", dateStart,
				dateEnd, CubeBuildTypeEnum.BUILD));
		waitCubeBuilt(jobs);
	}

	/**
	 * For cube test_kylin_cube_without_slr_left_join_empty, it is using
	 * update_insert, we will create 2 segments, and then merge these 2 segments
	 * into a larger segment For cube test_kylin_cube_with_slr_left_join_empty,
	 * we will create only 1 segment
	 * 
	 * @throws Exception
	 */
	private void testLeftJoinCube() throws Exception {
		this.prepareTestData("left");// default settings;

		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		f.setTimeZone(TimeZone.getTimeZone("GMT"));
		long dateStart;
		long dateEnd;

		ArrayList<String> jobs = new ArrayList<String>();

		// this cube's start date is 0, end date is 20501112000000
		CubeManager cubeMgr = CubeManager.getInstance(KylinConfig
				.getInstanceFromEnv());
		dateStart = cubeMgr.getCube("test_kylin_cube_with_slr_left_join_empty")
				.getDescriptor().getCubePartitionDesc().getPartitionDateStart();
		dateEnd = f.parse("2050-11-12").getTime();
		jobs.addAll(this.submitJob("test_kylin_cube_with_slr_left_join_empty",
				dateStart, dateEnd, CubeBuildTypeEnum.BUILD));

		// this cube's start date is 0, end date is 20120601000000
		dateStart = cubeMgr
				.getCube("test_kylin_cube_without_slr_left_join_empty")
				.getDescriptor().getCubePartitionDesc().getPartitionDateStart();
		dateEnd = f.parse("2012-06-01").getTime();
		jobs.addAll(this.submitJob(
				"test_kylin_cube_without_slr_left_join_empty", dateStart,
				dateEnd, CubeBuildTypeEnum.BUILD));

		waitCubeBuilt(jobs);

		jobs.clear();
		// then submit a update_insert job, start date is 20120101000000, end
		// date is 20220101000000
		dateStart = f.parse("2012-03-01").getTime();
		dateEnd = f.parse("2022-01-01").getTime();
		jobs.addAll(this.submitJob(
				"test_kylin_cube_without_slr_left_join_empty", dateStart,
				dateEnd, CubeBuildTypeEnum.BUILD));

		waitCubeBuilt(jobs);

		jobs.clear();
		// final submit a merge job, start date is 0, end date is 20220101000000
		dateEnd = f.parse("2022-01-01").getTime();
		jobs.addAll(this.submitJob(
				"test_kylin_cube_without_slr_left_join_empty", 0, dateEnd,
				CubeBuildTypeEnum.MERGE));
		waitCubeBuilt(jobs);
	}

	protected void waitCubeBuilt(List<String> jobs) throws Exception {

		boolean allFinished = false;
		while (!allFinished) {
			// sleep for 1 minutes
			Thread.sleep(60 * 1000L);

			allFinished = true;
			for (String job : jobs) {
				JobInstance savedJob = jobManager.getJob(job);
				JobStatusEnum jobStatus = savedJob.getStatus();
				if (jobStatus.getCode() <= JobStatusEnum.RUNNING.getCode()) {
					allFinished = false;
					break;
				}
			}
		}

		for (String job : jobs)
			assertEquals("Job fail - " + job, JobStatusEnum.FINISHED,
					jobManager.getJob(job).getStatus());
	}

	protected List<String> submitJob(String cubename, long startDate,
			long endDate, CubeBuildTypeEnum jobType) throws SchedulerException,
			IOException, InvalidJobInstanceException, CubeIntegrityException {
		List<String> jobList = new ArrayList<String>();

		CubeManager cubeMgr = CubeManager.getInstance(KylinConfig
				.getInstanceFromEnv());
		CubeInstance cube = cubeMgr.getCube(cubename);
		CubeManager.getInstance(this.getTestConfig()).loadCubeCache(cube);

		System.out.println(JsonUtil.writeValueAsIndentString(cube));
		List<CubeSegment> newSegments = cubeMgr.allocateSegments(cube, jobType,
				startDate, endDate);
		System.out.println(JsonUtil.writeValueAsIndentString(cube));

		for (CubeSegment seg : newSegments) {
			JobInstance newJob = jobManager.createJob(cubename, seg.getName(),
					jobType);
			// submit job to store
			String jobUuid = jobManager.submitJob(newJob);
			jobList.add(jobUuid);
		}

		return jobList;
	}
}