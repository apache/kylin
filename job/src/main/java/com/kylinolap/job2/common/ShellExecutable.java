package com.kylinolap.job2.common;

import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/**
 * Created by qianzhou on 12/26/14.
 */
public class ShellExecutable extends AbstractExecutable {

    private static final String CMD = "cmd";

    public ShellExecutable() {
    }

    public ShellExecutable(JobPO job, JobOutputPO jobOutput) {
        super(job, jobOutput);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            logger.info("executing:" + getCmd());
            final Pair<Integer, String> result = context.getConfig().getCliCommandExecutor().execute(getCmd());
            return new ExecuteResult(result.getFirst() == 0, result.getSecond());
        } catch (IOException e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(false, e.getLocalizedMessage());
        }
    }

    public void setCmd(String cmd) {
        setParam(CMD, cmd);
    }

    private String getCmd() {
        return getParam(CMD);
    }

    @Override
    public boolean isRunnable() {
        return getStatus() == ExecutableStatus.READY;
    }
}
