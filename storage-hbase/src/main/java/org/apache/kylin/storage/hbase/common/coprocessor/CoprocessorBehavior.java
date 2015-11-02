package org.apache.kylin.storage.hbase.common.coprocessor;

/**
 */
public enum CoprocessorBehavior {
    SCAN, //only scan data, used for profiling tuple scan speed. Will not return any result
    SCAN_FILTER, //only scan+filter used,used for profiling filter speed.  Will not return any result
    SCAN_FILTER_AGGR, //aggregate the result.  Will return results
    SCAN_FILTER_AGGR_CHECKMEM, //default full operations. Will return results
}
