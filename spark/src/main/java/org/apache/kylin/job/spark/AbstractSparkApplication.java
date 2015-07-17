package org.apache.kylin.job.spark;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.job.tools.OptionsHelper;

import java.io.Serializable;

/**
 */
public abstract class AbstractSparkApplication implements Serializable {

    protected abstract Options getOptions();

    protected abstract void execute(OptionsHelper optionsHelper) throws Exception;

    public final void execute(String[] args) {
        OptionsHelper optionsHelper = new OptionsHelper();
        System.out.println("Spark Application args:" + StringUtils.join(args, " "));
        try {
            optionsHelper.parseOptions(getOptions(), args);
            execute(optionsHelper);
        } catch (ParseException e) {
            optionsHelper.printUsage("SparkExecutor", getOptions());
            throw new RuntimeException("error parsing args", e);
        } catch (Exception e) {
            throw new RuntimeException("error execute Spark Application", e);
        }
    }
}
