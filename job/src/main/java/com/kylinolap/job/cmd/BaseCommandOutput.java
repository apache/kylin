package com.kylinolap.job.cmd;

/**
 * Created by qianzhou on 12/4/14.
 */
public abstract class BaseCommandOutput implements ICommandOutput {

    @Override
    public void log(String message) {
        this.appendOutput(message);
    }
}
