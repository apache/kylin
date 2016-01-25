package org.apache.kylin.job.hadoop.cube;

import org.apache.hadoop.util.ToolRunner;

/**
 * Created by liuze on 2016/1/13 0013.
 */
public class BulkESJob extends CuboidJob {
    public BulkESJob() {
        this.setMapperClass(BulkESMapper.class);
    }

    public static void main(String[] args) throws Exception {
        CuboidJob job = new BulkESJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
