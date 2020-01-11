package org.apache.kylin.common;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.CompressionUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.zip.DataFormatException;

/**
 * KylinCodecTest
 * created by haihuang.hhl @2020/1/9
 */
public class KylinCodecTest {
    public static byte[] mockData(int loop) {
        int base = 1000;
        int num = (int) (base * Math.pow(10, loop));
        byte[] data = new byte[num];
        Random random = new Random();
        for (int i = 0; i < num; i++) {
            data[i] = (byte) random.nextInt(2);
        }
        return data;
    }
    @Test
    public void test() throws Exception {
        for (int i = 0; i <= 6; i++) {
            byte[] data = mockData(i);
            String[] algorithms = new String[]{"lz4", "zstd", ""};
            for (String algo : algorithms) {
                long start = System.currentTimeMillis();
                byte[] compressed = CompressionUtils.compress(data, algo);
                long start_c = System.currentTimeMillis();
                byte[] decompressed = CompressionUtils.decompress(compressed, algo);
                long end_dc = System.currentTimeMillis();
                assert Bytes.compareTo(data, decompressed) == 0;
                if ("".equals(algo)) {
                    algo = "java-zip";
                }
                double compress_ratio = 0;
                if (compressed.length > 0) {
                    compress_ratio = (double) data.length / compressed.length;
                }
                System.out.println("algo " + algo + " original size:" + data.length +
                        " after:" + compressed.length + " compress ratio:" + String.format("%.2f", compress_ratio)
                        + " compress time cost:" +
                        (start_c - start) + " decompress time cost:" + (end_dc - start_c));
            }
        }

    }


}
