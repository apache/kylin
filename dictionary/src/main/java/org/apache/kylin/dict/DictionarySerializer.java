package org.apache.kylin.dict;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.ClassUtil;

import java.io.*;

/**
 * Created by qianzhou on 5/5/15.
 */
public final class DictionarySerializer {

    private DictionarySerializer() {}

    public static Dictionary<?> deserialize(InputStream inputStream) {
        try {
            final DataInputStream dataInputStream = new DataInputStream(inputStream);
            final String type = dataInputStream.readUTF();
            final Dictionary dictionary = ClassUtil.forName(type, Dictionary.class).newInstance();
            dictionary.readFields(dataInputStream);
            return dictionary;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Dictionary<?> deserialize(ImmutableBytesWritable dictBytes) {
        return deserialize(new ByteArrayInputStream(dictBytes.get(), dictBytes.getOffset(), dictBytes.getLength()));
    }

    public static void serialize(Dictionary<?> dict, OutputStream outputStream) {
        try {
            DataOutputStream out = new DataOutputStream(outputStream);
            out.writeUTF(dict.getClass().getName());
            dict.write(out);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ImmutableBytesWritable serialize(Dictionary<?> dict) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeUTF(dict.getClass().getName());
            dict.write(out);
            return new ImmutableBytesWritable(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
