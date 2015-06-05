package org.apache.kylin.dict;

import org.apache.kylin.common.util.DateFormat;

import java.io.*;

/**
 */
public class TimeStrDictionary extends Dictionary<String> {

    private long maxid = Integer.MAX_VALUE;
    private int maxLenghOfPositiveLong = 19;

    @Override
    public int getMinId() {
        return 0;
    }

    @Override
    public int getMaxId() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSizeOfId() {
        return 4;
    }

    @Override
    public int getSizeOfValue() {
        return maxLenghOfPositiveLong;
    }

    @Override
    protected int getIdFromValueImpl(String value, int roundingFlag) {
        long millis = DateFormat.stringToMillis(value);
        long seconds = millis / 1000;

        if (seconds > maxid) {
            return nullId();
        } else if (seconds < 0) {
            throw new IllegalArgumentException("Illegal value: " + value + ", parsed seconds: " + seconds);
        }

        return (int) seconds;
    }

    /**
     *
     * @param id
     * @return return like "0000001430812800000"
     */
    @Override
    protected String getValueFromIdImpl(int id) {
        if (id == nullId())
            return null;

        long millis = 1000L * id;
        String s = Long.toString(millis);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < maxLenghOfPositiveLong - s.length(); ++i) {
            sb.append('0');
        }
        sb.append(s);
        return sb.toString();
    }

    @Override
    final protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        try {
            return getIdFromValue(new String(value, offset, len, "ISO-8859-1"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
    }

    @Override
    final protected byte[] getValueBytesFromIdImpl(int id) {
        String date = getValueFromId(id);
        byte bytes[];
        try {
            bytes = date.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
        return bytes;
    }

    @Override
    final protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset) {
        byte bytes[] = getValueBytesFromIdImpl(id);
        System.arraycopy(bytes, 0, returnValue, offset, bytes.length);
        return bytes.length;
    }

    @Override
    public void dump(PrintStream out) {
        out.println(this.toString());
    }

    @Override
    public String toString() {
        return "TimeStrDictionary supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07 (does not support millisecond)";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;

        return o instanceof TimeStrDictionary;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
}
