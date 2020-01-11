package org.apache.kylin.common.codec;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * KylinCodecUtil
 * created by haihuang.hhl @2020/1/9
 */
public class KylinCodecUtil {
    public static byte[] compress(byte[] uncompressed, String codecName) throws Exception {
        ByteArrayOutputStream compressBos = new ByteArrayOutputStream();
        try (OutputStream out = KylinCodecFactory.createCodec(codecName).compressedOutputStream(compressBos)) {
            out.write(uncompressed);
        }
        return compressBos.toByteArray();
    }

    public static byte[] decompress(byte[] compressed, String codecName) throws Exception {
        ByteArrayInputStream deCompressBis = new ByteArrayInputStream(compressed);
        try (InputStream in = KylinCodecFactory.createCodec(codecName).compressedInputStream(deCompressBis)) {
            return IOUtils.toByteArray(in);
        }

    }
}