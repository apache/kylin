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

package com.kylinolap.rest.metrics;

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.kylinolap.job.JobManager;

/**
 * @author xduo
 * 
 */
public class JobMetrics implements MetricSet {

	private JobManager jobManager;

	static class JobMetricsHolder {
		static final JobMetrics INSTANCE = new JobMetrics();
	}

	public static JobMetrics getInstance() {
		return JobMetricsHolder.INSTANCE;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.codahale.metrics.MetricSet#getMetrics()
	 */
	@Override
	public Map<String, Metric> getMetrics() {
		Map<String, Metric> metricSet = new HashMap<String, Metric>();

		metricSet.put("PercentileJobStepDuration", new Gauge<Double>() {
			@Override
			public Double getValue() {
				return jobManager.getPercentileJobStepDuration(95);
			}
		});

		metricSet.put("scheduledJobs", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return jobManager.getScheduledJobsSzie();
			}
		});

		metricSet.put("EngineThreadPool", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return jobManager.getEngineThreadPoolSize();
			}
		});

		metricSet.put("MaxJobStep", new Gauge<Double>() {
			@Override
			public Double getValue() {
				return jobManager.getMaxJobStepDuration();
			}
		});

		metricSet.put("MinJobStep", new Gauge<Double>() {
			@Override
			public Double getValue() {
				return jobManager.getMinJobStepDuration();
			}
		});

		metricSet.put("IdleSlots", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return jobManager.getNumberOfIdleSlots();
			}
		});

		metricSet.put("JobStepsExecuted", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return jobManager.getNumberOfJobStepsExecuted();
			}
		});

		metricSet.put("JobStepsRunning", new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return jobManager.getNumberOfJobStepsRunning();
			}
		});

		return metricSet;
	}

	public void setJobManager(JobManager jobManager) {
		this.jobManager = jobManager;
	}

}
