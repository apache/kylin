package org.apache.kylin.job.spark;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.job.common.OptionsHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 */
public class SparkCubing extends AbstractSparkApplication {

    private static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName("path").hasArg().isRequired(true).withDescription("Hive Intermediate Table").create("hiveTable");

    private Options options;

    public SparkCubing() {
        options = new Options();
        options.addOption(OPTION_INPUT_PATH);

    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());
        final DataFrame dataFrame = sqlContext.sql("select * from " + hiveTable);
        System.out.println("total record count:" + dataFrame.count());
    }
}
