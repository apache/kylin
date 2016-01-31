package org.apache.kylin.engine.mr.steps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.common.util.Bytes;

/**
 */
public class FactDistinctColumnPartitioner extends Partitioner<Text, Text> {
    private Configuration conf;

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {

        long colIndex = Bytes.toLong(key.getBytes(), 0, Bytes.SIZEOF_LONG);
        if (colIndex < 0) {
            // the last reducer is for merging hll
            return numReduceTasks - 1;
        } else {
            return (int) (colIndex);
        }

    }

}
