package org.apache.kylin.storage.hbase.coprocessor.observer;

/**
 */
public enum ObserverBehavior {
    SCAN, //only scan data, used for profiling tuple scan speed
    SCAN_FILTER, //only scan+filter used,used for profiling filter speed
    SCAN_FILTER_AGGR, //default normal behavior
}
