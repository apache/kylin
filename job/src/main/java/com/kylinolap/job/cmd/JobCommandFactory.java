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

import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobStepCmdTypeEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.hadoop.cube.BaseCuboidMapper;
import com.kylinolap.job.hadoop.cube.CubeHFileJob;
import com.kylinolap.job.hadoop.cube.CuboidJob;
import com.kylinolap.job.hadoop.cube.FactDistinctColumnsJob;
import com.kylinolap.job.hadoop.cube.MergeCuboidJob;
import com.kylinolap.job.hadoop.cube.NDCuboidMapper;
import com.kylinolap.job.hadoop.cube.RangeKeyDistributionJob;
import com.kylinolap.job.hadoop.dict.CreateDictionaryJob;
import com.kylinolap.job.hadoop.hbase.BulkLoadJob;
import com.kylinolap.job.hadoop.hbase.CreateHTableJob;

/**
 * @author xjiang
 * 
 */
public class JobCommandFactory {

	public static IJobCommand getJobCommand(String command,
			JobInstance jobInstance, int jobStepID, JobEngineConfig engineConfig) {
		String instanceID = jobInstance.getUuid();

		boolean isRemote = engineConfig.isRunAsRemoteCommand();
		String hostname = isRemote ? engineConfig.getRemoteHadoopCliHostname()
				: null;
		String username = isRemote ? engineConfig.getRemoteHadoopCliUsername()
				: null;
		String password = isRemote ? engineConfig.getRemoteHadoopCliPassword()
				: null;
		JobStep jobStep = jobInstance.getSteps().get(jobStepID);
		boolean isAsync = jobStep.isRunAsync();
		JobStepCmdTypeEnum type = jobStep.getCmdType();

		switch (type) {
		case SHELL_CMD_HADOOP:
			return new ShellHadoopCmd(command, hostname, username, password,
					isAsync, instanceID, jobStepID, engineConfig);
		case JAVA_CMD_HADOOP_FACTDISTINCT:
			FactDistinctColumnsJob factDistinctJob = new FactDistinctColumnsJob();
			factDistinctJob.setAsync(isAsync);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, factDistinctJob, isAsync);
		case JAVA_CMD_HADOOP_BASECUBOID:
			CuboidJob baseCuboidJob = new CuboidJob();
			baseCuboidJob.setAsync(isAsync);
			baseCuboidJob.setMapperClass(BaseCuboidMapper.class);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, baseCuboidJob, isAsync);
		case JAVA_CMD_HADOOP_NDCUBOID:
			CuboidJob ndCuboidJob = new CuboidJob();
			ndCuboidJob.setAsync(isAsync);
			ndCuboidJob.setMapperClass(NDCuboidMapper.class);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, ndCuboidJob, isAsync);
		case JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION:
			AbstractHadoopJob rangeKeyDistributionJob = new RangeKeyDistributionJob();
			rangeKeyDistributionJob.setAsync(isAsync);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, rangeKeyDistributionJob, isAsync);
		case JAVA_CMD_HADOOP_CONVERTHFILE:
			CubeHFileJob cubeHFileJob = new CubeHFileJob();
			cubeHFileJob.setAsync(isAsync);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, cubeHFileJob, isAsync);
		case JAVA_CMD_HADOOP_MERGECUBOID:
			MergeCuboidJob mergeCuboidJob = new MergeCuboidJob();
			mergeCuboidJob.setAsync(isAsync);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, mergeCuboidJob, isAsync);
		case JAVA_CMD_HADOOP_NO_MR_DICTIONARY:
			CreateDictionaryJob createDictionaryJob = new CreateDictionaryJob();
			createDictionaryJob.setAsync(isAsync);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, createDictionaryJob, isAsync);
		case JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE:
			CreateHTableJob createHTableJob = new CreateHTableJob();
			createHTableJob.setAsync(isAsync);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, createHTableJob, isAsync);
		case JAVA_CMD_HADOOP_NO_MR_BULKLOAD:
			BulkLoadJob bulkLoadJob = new BulkLoadJob();
			bulkLoadJob.setAsync(isAsync);
			return new JavaHadoopCmd(command, instanceID, jobStepID,
					engineConfig, bulkLoadJob, isAsync);
		default:
			return new ShellCmd(command, hostname, username, password, isAsync);
		}
	}
}
