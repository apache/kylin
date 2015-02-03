package org.apache.kylin.common.util;

/**
 * Created by Hongbin Ma(Binmahone) on 1/27/15.
 */
public class LogTitlePrinter {
    public static void printTitle(String title) {
        String leftAlignFormat = "| %-100s | %n";

        System.out.format("+------------------------------------------------------------------------------------------------------+%n");
        System.out.format(leftAlignFormat, title);
        System.out.format("+------------------------------------------------------------------------------------------------------+%n");
    }
}
