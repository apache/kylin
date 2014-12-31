package com.kylinolap.job2.common;

import com.google.common.base.Preconditions;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import org.apache.hadoop.util.ToolRunner;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Created by qianzhou on 12/25/14.
 */
public class MapReduceExecutable extends AbstractExecutable {

    private static final String KEY_MR_JOB = "MR_JOB_CLASS";
    private static final String KEY_PARAMS = "MR_JOB_PARAMS";

    public MapReduceExecutable() {
    }

    public MapReduceExecutable(JobPO job) {
        super(job);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final String mapReduceJobClass = getMapReduceJobClass();
        String params = getMapReduceParams();
        Preconditions.checkNotNull(mapReduceJobClass);
        Preconditions.checkNotNull(params);
        try {
            final Constructor<? extends AbstractHadoopJob> constructor = (Constructor<? extends AbstractHadoopJob>) Class.forName(mapReduceJobClass).getConstructor();
            final AbstractHadoopJob job = constructor.newInstance();
            job.setAsync(true);
            String[] args = params.trim().split("\\s+");
            ToolRunner.run(job, args);

            final HadoopCmdOutput hadoopCmdOutput = new HadoopCmdOutput(context.getConfig().getYarnStatusServiceUrl(), job);
            JobStepStatusEnum status;
            do {
                status = hadoopCmdOutput.getStatus();
                jobService.updateJobInfo(getId(), job.getInfo());
                if (status.isComplete()) {
                    final Map<String, String> info = job.getInfo();
                    info.put(JobInstance.SOURCE_RECORDS_COUNT, hadoopCmdOutput.getMapInputRecords());
                    info.put(JobInstance.HDFS_BYTES_WRITTEN, hadoopCmdOutput.getHdfsBytesWritten());
                    jobService.updateJobInfo(getId(), info);

                    if (status == JobStepStatusEnum.FINISHED) {
                        return new ExecuteResult(ExecuteResult.State.SUCCEED, hadoopCmdOutput.getOutput());
                    } else {
                        return new ExecuteResult(ExecuteResult.State.FAILED, hadoopCmdOutput.getOutput());
                    }
                }
                Thread.sleep(context.getConfig().getYarnStatusCheckIntervalSeconds() * 1000);
            } while (!isStopped());

            return new ExecuteResult(ExecuteResult.State.STOPPED, hadoopCmdOutput.getOutput());

        } catch (ReflectiveOperationException e) {
            logger.error("error getMapReduceJobClass, class name:" + getParam(KEY_MR_JOB), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        } catch (Exception e) {
            logger.error("error execute MapReduceJob, id:" + getId(), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    public void setMapReduceJobClass(Class<? extends AbstractHadoopJob> clazzName) {
        setParam(KEY_MR_JOB, clazzName.getName());
    }

    String getMapReduceJobClass() throws ExecuteException {
        return getParam(KEY_MR_JOB);
    }

    public void setMapReduceParams(String param) {
        setParam(KEY_PARAMS, param);
    }

    protected String getMapReduceParams() {
        return getParam(KEY_PARAMS);
    }

}
