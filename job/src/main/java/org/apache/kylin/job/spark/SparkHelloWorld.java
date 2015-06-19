package org.apache.kylin.job.spark;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.tools.OptionsHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;

/**
 */
public class SparkHelloWorld extends AbstractSparkApplication {

    @Override
    protected Options getOptions() {
        return new Options();
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        System.out.println("hello kylin-spark");
    }
}
