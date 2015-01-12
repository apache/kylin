package com.kylinolap.job.execution;

/**
 * Created by qianzhou on 12/16/14.
 */
public interface Async {

    int checkInterval();

    void onResult(Executable executable, ExecuteResult result);

}
