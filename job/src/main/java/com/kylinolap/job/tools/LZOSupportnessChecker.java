package com.kylinolap.job.tools;

import org.apache.hadoop.hbase.util.CompressionTest;

import java.io.File;

/**
 * Created by honma on 10/21/14.
 */
public class LZOSupportnessChecker {
    public static boolean getSupportness() {
        try {
            File temp = File.createTempFile("test", ".tmp");
            CompressionTest.main(new String[] { "file://" + temp.toString(), "lzo" });
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
