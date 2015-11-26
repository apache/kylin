package org.apache.kylin.dict;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;

/**
 */
public final class DictionarySerializer {

    private DictionarySerializer() {
    }

    public static Dictionary<?> deserialize(InputStream inputStream) {
        try {
            final DataInputStream dataInputStream = new DataInputStream(inputStream);
            final String type = dataInputStream.readUTF();
            final Dictionary<?> dictionary = ClassUtil.forName(type, Dictionary.class).newInstance();
            dictionary.readFields(dataInputStream);
            return dictionary;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Dictionary<?> deserialize(ByteArray dictBytes) {
        return deserialize(new ByteArrayInputStream(dictBytes.array(), dictBytes.offset(), dictBytes.length()));
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

    public static ByteArray serialize(Dictionary<?> dict) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeUTF(dict.getClass().getName());
            dict.write(out);
            return new ByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
