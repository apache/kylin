package com.kylinolap.job.deployment;

/**
 * Created by honma on 9/29/14.
 */
public enum DeploymentUtilityChecker {
    HIVE_CHECKER {
        @Override
        boolean isOkay() {
            return true;
        }
    },
    HBASE_CHECKER {
        @Override
        boolean isOkay() {
            return true;
        }
    };


    abstract boolean isOkay();
}
