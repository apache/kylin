package org.apache.kylin.job.spark;

import org.apache.commons.cli.Options;
import org.apache.kylin.job.common.OptionsHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 */
public class SparkHiveDemo extends AbstractSparkApplication {

    private final Options options;

    public SparkHiveDemo() {
        options = new Options();
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());
        final DataFrame dataFrame = sqlContext.sql("select * from test_kylin_fact");
        System.out.println("count * of the table:" + dataFrame.count());
    }
}
