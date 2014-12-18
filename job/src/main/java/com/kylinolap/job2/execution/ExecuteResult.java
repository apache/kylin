package com.kylinolap.job2.execution;

/**
 * Created by qianzhou on 12/15/14.
 */
public final class ExecuteResult {

    private final boolean succeed;
    private final String output;

    public ExecuteResult(boolean succeed, String output) {
        this.succeed = succeed;
        this.output = output;
    }

    public boolean succeed() {
        return succeed;
    }

    public String output() {
        return output;
    }
}
