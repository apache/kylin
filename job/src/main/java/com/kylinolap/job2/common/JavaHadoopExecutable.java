package com.kylinolap.job2.common;

import com.kylinolap.common.util.CliCommandExecutor;
import com.kylinolap.job2.cube.AbstractBuildCubeJob;
import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;

import java.io.IOException;

/**
 * Created by qianzhou on 12/25/14.
 */
public class JavaHadoopExecutable extends AbstractBuildCubeJob {

    private static final String SHELL_CMD = "shellCmd";

    private CliCommandExecutor cliCommandExecutor = new CliCommandExecutor();

    public JavaHadoopExecutable(JobPO job, JobOutputPO jobOutput) {
        super(job, jobOutput);
    }

    void setShellCmd(String cmd) {
        setParam(SHELL_CMD, cmd);
    }

    public String getShellCmd() {
        return getParam(SHELL_CMD);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Integer result = cliCommandExecutor.execute(getShellCmd(), null).getFirst();
//            if (exitCode == 0) {
//                output.setStatus(JobStepStatusEnum.FINISHED);
//            } else if (exitCode == -2) {
//                output.setStatus(JobStepStatusEnum.DISCARDED);
//            } else {
//                output.setStatus(JobStepStatusEnum.ERROR);
//            }
//            output.setExitCode(exitCode);
            if (result == 0) {
                return new ExecuteResult(true, null);
            } else {
                return new ExecuteResult(false, "");
            }
        } catch (IOException e) {
            throw new ExecuteException(e);
        }
    }

    @Override
    public boolean isRunnable() {
        return false;
    }
}
