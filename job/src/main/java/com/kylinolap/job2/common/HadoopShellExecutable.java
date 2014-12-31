package com.kylinolap.job2.common;

import com.google.common.base.Preconditions;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * Created by qianzhou on 12/26/14.
 */
public class HadoopShellExecutable extends AbstractExecutable {

    private static final String KEY_MR_JOB = "HADOOP_SHELL_JOB_CLASS";
    private static final String KEY_PARAMS = "HADOOP_SHELL_JOB_PARAMS";

    public HadoopShellExecutable() {
    }

    public HadoopShellExecutable(JobPO job) {
        super(job);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final String mapReduceJobClass = getJobClass();
        String params = getMapReduceParams();
        Preconditions.checkNotNull(mapReduceJobClass);
        Preconditions.checkNotNull(params);
        try {
            final Constructor<? extends AbstractHadoopJob> constructor = (Constructor<? extends AbstractHadoopJob>) Class.forName(mapReduceJobClass).getConstructor();
            final AbstractHadoopJob job = constructor.newInstance();
            job.setAsync(true);
            String[] args = params.trim().split("\\s+");
            final int result = ToolRunner.run(job, args);
            return result == 0 ? new ExecuteResult(ExecuteResult.State.SUCCEED, ""):new ExecuteResult(ExecuteResult.State.FAILED, "result code:" + result);
        } catch (ReflectiveOperationException e) {
            logger.error("error getMapReduceJobClass, class name:" + getParam(KEY_MR_JOB), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        } catch (Exception e) {
            logger.error("error execute MapReduceJob, id:" + getId(), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    public void setJobClass(Class<? extends AbstractHadoopJob> clazzName) {
        setParam(KEY_MR_JOB, clazzName.getName());
    }

    String getJobClass() throws ExecuteException {
        return getParam(KEY_MR_JOB);
    }

    public void setMapReduceParams(String param) {
        setParam(KEY_PARAMS, param);
    }

    protected String getMapReduceParams() {
        return getParam(KEY_PARAMS);
    }

}
