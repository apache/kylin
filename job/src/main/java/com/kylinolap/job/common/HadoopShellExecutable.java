package com.kylinolap.job.common;

import java.lang.reflect.Constructor;

import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;
import com.kylinolap.job.exception.ExecuteException;
import com.kylinolap.job.execution.ExecutableContext;
import com.kylinolap.job.execution.ExecuteResult;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.execution.AbstractExecutable;

/**
 * Created by qianzhou on 12/26/14.
 */
public class HadoopShellExecutable extends AbstractExecutable {

    private static final String KEY_MR_JOB = "HADOOP_SHELL_JOB_CLASS";
    private static final String KEY_PARAMS = "HADOOP_SHELL_JOB_PARAMS";

    public HadoopShellExecutable() {
        super();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final String mapReduceJobClass = getJobClass();
        String params = getJobParams();
        Preconditions.checkNotNull(mapReduceJobClass);
        Preconditions.checkNotNull(params);
        try {
            final Constructor<? extends AbstractHadoopJob> constructor = (Constructor<? extends AbstractHadoopJob>) Class.forName(mapReduceJobClass).getConstructor();
            final AbstractHadoopJob job = constructor.newInstance();
            String[] args = params.trim().split("\\s+");
            logger.info("parameters of the HadoopShellExecutable:");
            logger.info(params);
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

    public String getJobClass() throws ExecuteException {
        return getParam(KEY_MR_JOB);
    }

    public void setJobParams(String param) {
        setParam(KEY_PARAMS, param);
    }

    public String getJobParams() {
        return getParam(KEY_PARAMS);
    }

}
