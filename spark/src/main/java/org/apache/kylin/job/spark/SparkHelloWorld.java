package org.apache.kylin.job.spark;

import org.apache.commons.cli.Options;
import org.apache.kylin.job.common.OptionsHelper;

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
