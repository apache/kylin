package com.kylinolap.job2.execution;

/**
 * Created by qianzhou on 12/16/14.
 */
public interface Async {

    int checkInterval();

    void onResult(Executable executable, ExecuteResult result);

}
