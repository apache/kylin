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

package com.kylinolap.job.cmd;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.tools.HadoopStatusChecker;

/**
 * @author xjiang
 * 
 */
public class ShellHadoopCmdOutput extends ShellCmdOutput {

	// mr job
	private static final Pattern PATTERN_APP_ID = Pattern
			.compile("Submitted application (.*?) to ResourceManager");
	private static final Pattern PATTERN_APP_URL = Pattern
			.compile("The url to track the job: (.*)");
	private static final Pattern PATTERN_JOB_ID = Pattern
			.compile("Running job: (.*)");
	private static final Pattern PATTERN_HDFS_BYTES_WRITTEN = Pattern
			.compile("HDFS: Number of bytes written=(\\d+)");
	private static final Pattern PATTERN_SOURCE_RECORDS_COUNT = Pattern
			.compile("Map input records=(\\d+)");
	private static final Pattern PATTERN_SOURCE_RECORDS_SIZE = Pattern
			.compile("HDFS Read: (\\d+) HDFS Write");

	// hive
	private static final Pattern PATTERN_HIVE_APP_ID_URL = Pattern
			.compile("Starting Job = (.*?), Tracking URL = (.*)");
	private static final Pattern PATTERN_HIVE_BYTES_WRITTEN = Pattern
			.compile("HDFS Read: (\\d+) HDFS Write: (\\d+) SUCCESS");

	private final KylinConfig config;
	private final String jobInstanceID;
	private final int jobStepID;
	private final String yarnUrl;

	private String mrJobID;

	public ShellHadoopCmdOutput(String jobInstanceID, int jobStepID,
			JobEngineConfig engineConfig) {
		super();
		this.config = engineConfig.getConfig();
		this.yarnUrl = engineConfig.getYarnStatusServiceUrl();
		this.jobInstanceID = jobInstanceID;
		this.jobStepID = jobStepID;
		this.mrJobID = null;
	}

	@Override
	public JobStepStatusEnum getStatus() {
		if (status.isComplete()) {
			return status;
		}

		if (null == mrJobID || mrJobID.trim().length() == 0) {
			return JobStepStatusEnum.WAITING;
		}

		HadoopStatusChecker hadoopJobChecker = new HadoopStatusChecker(
				this.yarnUrl, this.mrJobID, output);
		status = hadoopJobChecker.checkStatus();

		return status;
	}

	@Override
	public void appendOutput(String message) {
		super.appendOutput(message);
		try {
			updateJobStepInfo(message);
		} catch (IOException e) {
			log.error("Failed to append output!\n" + message, e);
			status = JobStepStatusEnum.ERROR;
		}

	}

	private void updateJobStepInfo(final String message) throws IOException {

		JobDAO jobDAO = JobDAO.getInstance(config);
		JobInstance jobInstance = jobDAO.getJob(jobInstanceID);
		JobStep jobStep = jobInstance.getSteps().get(jobStepID);

		Matcher matcher = PATTERN_APP_ID.matcher(message);
		if (matcher.find()) {
			String appId = matcher.group(1);
			jobStep.putInfo(JobInstance.YARN_APP_ID, appId);
			jobDAO.updateJobInstance(jobInstance);
		}

		matcher = PATTERN_APP_URL.matcher(message);
		if (matcher.find()) {
			String appTrackingUrl = matcher.group(1);
			jobStep.putInfo(JobInstance.YARN_APP_URL, appTrackingUrl);
			jobDAO.updateJobInstance(jobInstance);
		}

		matcher = PATTERN_JOB_ID.matcher(message);
		if (matcher.find()) {
			String mrJobID = matcher.group(1);
			jobStep.putInfo(JobInstance.MR_JOB_ID, mrJobID);
			jobDAO.updateJobInstance(jobInstance);
			this.mrJobID = mrJobID;
			log.debug("Get hadoop job id " + mrJobID);
		}

		matcher = PATTERN_HDFS_BYTES_WRITTEN.matcher(message);
		if (matcher.find()) {
			String hdfsWritten = matcher.group(1);
			jobStep.putInfo(JobInstance.HDFS_BYTES_WRITTEN, hdfsWritten);
			jobDAO.updateJobInstance(jobInstance);
		}

		matcher = PATTERN_SOURCE_RECORDS_COUNT.matcher(message);
		if (matcher.find()) {
			String sourceCount = matcher.group(1);
			jobStep.putInfo(JobInstance.SOURCE_RECORDS_COUNT, sourceCount);
			jobDAO.updateJobInstance(jobInstance);
		}

		matcher = PATTERN_SOURCE_RECORDS_SIZE.matcher(message);
		if (matcher.find()) {
			String sourceSize = matcher.group(1);
			jobStep.putInfo(JobInstance.SOURCE_RECORDS_SIZE, sourceSize);
			jobDAO.updateJobInstance(jobInstance);
		}

		// hive
		matcher = PATTERN_HIVE_APP_ID_URL.matcher(message);
		if (matcher.find()) {
			String jobId = matcher.group(1);
			String trackingUrl = matcher.group(2);
			jobStep.putInfo(JobInstance.MR_JOB_ID, jobId);
			jobStep.putInfo(JobInstance.YARN_APP_URL, trackingUrl);
			jobDAO.updateJobInstance(jobInstance);
		}

		matcher = PATTERN_HIVE_BYTES_WRITTEN.matcher(message);
		if (matcher.find()) {
			// String hdfsRead = matcher.group(1);
			String hdfsWritten = matcher.group(2);
			jobStep.putInfo(JobInstance.HDFS_BYTES_WRITTEN, hdfsWritten);
			jobDAO.updateJobInstance(jobInstance);
		}
	}
}
