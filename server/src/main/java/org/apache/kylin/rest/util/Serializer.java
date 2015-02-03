package org.apache.kylin.rest.util;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.kylin.common.util.JsonUtil;

public class Serializer<T> {
    private final Class<T> type;

    public Serializer(Class<T> type) {
        this.type = type;
    }

    public T deserialize(byte[] value) throws JsonParseException, JsonMappingException, IOException {
        if (null == value) {
            return null;
        }

        return JsonUtil.readValue(value, type);
    }

    public byte[] serialize(T obj) throws JsonProcessingException {
        if (null == obj) {
            return null;
        }

        return JsonUtil.writeValueAsBytes(obj);
    }
}
