package com.kylinolap.job2.execution;

import java.util.List;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface ChainedExecutable extends Executable {

    List<? extends Executable> getTasks();

}
