package org.apache.kylin.common.mr;


import org.apache.kylin.common.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Hongbin Ma(Binmahone) on 1/19/15.
 */
public class KylinMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    protected void publishConfiguration(Configuration conf) {
        HadoopUtil.setCurrentConfiguration(conf);
    }
}
