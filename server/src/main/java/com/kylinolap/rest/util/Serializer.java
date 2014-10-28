package com.kylinolap.rest.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Serializer<T> {
    private static final Logger logger = LoggerFactory.getLogger(Serializer.class);
    private final Class<T> type;

    public Serializer(Class<T> type) {
        this.type = type;
    }

    public T deserialize(byte[] value) {
        if (null == value) {
            return null;
        }

        try {
            ObjectMapper mapper = new ObjectMapper();

            return mapper.readValue(value, type);
        } catch (JsonParseException e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (JsonMappingException e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        return null;
    }

    public byte[] serialize(T obj) {
        if (null == obj) {
            return null;
        }

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(dout, obj);
            dout.close();
            buf.close();
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        return buf.toByteArray();
    }
}
