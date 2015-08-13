package org.apache.kylin.job.spark;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 */
public final class SparkEntry {

    public static void main(String[] args) throws Exception {
        System.out.println("SparkEntry args:" + StringUtils.join(args, " "));
        Preconditions.checkArgument(args.length >= 2, "-className is required");
        Preconditions.checkArgument(args[0].equals("-className"), "-className is required");
        final String className = args[1];
        final Object o = Class.<AbstractSparkApplication> forName(className).newInstance();
        Preconditions.checkArgument(o instanceof AbstractSparkApplication, className + " is not a subClass of AbstractSparkApplication");
        String[] appArgs = new String[args.length - 2];
        for (int i = 2; i < args.length; i++) {
            appArgs[i - 2] = args[i];
        }
        AbstractSparkApplication abstractSparkApplication = (AbstractSparkApplication) o;
        abstractSparkApplication.execute(appArgs);
    }
}
