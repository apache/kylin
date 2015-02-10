package com.kylinolap.common.mr;

import com.kylinolap.common.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Hongbin Ma(Binmahone) on 1/19/15.
 */
public class KylinReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    protected void publishConfiguration(Configuration conf) {
        HadoopUtil.setCurrentConfiguration(conf);
    }
}
