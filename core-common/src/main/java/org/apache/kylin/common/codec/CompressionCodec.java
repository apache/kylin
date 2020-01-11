package org.apache.kylin.common.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * CompressionCodec
 * created by haihuang.hhl @2019/9/25
 */
public interface CompressionCodec {

    OutputStream compressedOutputStream(OutputStream outputStream) throws IOException;

    InputStream compressedInputStream(InputStream inputStream) throws IOException;

    String getCompressionCodecName();

}
