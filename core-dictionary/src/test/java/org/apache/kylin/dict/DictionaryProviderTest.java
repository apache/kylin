package org.apache.kylin.dict;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by xiefan on 16-11-23.
 */
public class DictionaryProviderTest {

    @Test
    public void testReadWrite() throws Exception{
        //string dict
        Dictionary<String> dict = getDict(DataType.getType("string"),
                Arrays.asList(new String[]{"a","b"}).iterator());
        readWriteTest(dict);
        //number dict
        Dictionary<String> dict2 = getDict(DataType.getType("long"),
                Arrays.asList(new String[]{"1","2"}).iterator());
        readWriteTest(dict2);

        //date dict
        Dictionary<String> dict3 = getDict(DataType.getType("datetime"),
                Arrays.asList(new String[]{"20161122","20161123"}).iterator());
        readWriteTest(dict3);

        //date dict
        Dictionary<String> dict4 = getDict(DataType.getType("datetime"),
                Arrays.asList(new String[]{"2016-11-22","2016-11-23"}).iterator());
        readWriteTest(dict4);

        //date dict
        try {
            Dictionary<String> dict5 = getDict(DataType.getType("date"),
                    Arrays.asList(new String[]{"2016-11-22", "20161122"}).iterator());
            readWriteTest(dict5);
            fail("Date format not correct.Should throw exception");
        }catch (IllegalStateException e){
            //correct
        }
    }

    @Test
    public void testReadWriteTime(){
        System.out.println(Long.MAX_VALUE);
        System.out.println(Long.MIN_VALUE);
    }


    private Dictionary<String> getDict(DataType type, Iterator<String> values) throws Exception{
        IDictionaryReducerLocalBuilder builder = DictionaryReducerLocalGenerator.getBuilder(type);
        while(values.hasNext()){
            builder.addValue(values.next());
        }
        return builder.build(0);
    }

    private void readWriteTest(Dictionary<String> dict) throws Exception{
        final String path = "src/test/resources/dict/tmp_dict";
        File f = new File(path);
        f.deleteOnExit();
        f.createNewFile();
        String dictClassName = dict.getClass().getName();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f));
        out.writeUTF(dictClassName);
        dict.write(out);
        out.close();
        //read dict
        DataInputStream in = null;
        Dictionary<String> dict2 = null;
        try {
            File f2 = new File(path);
            in = new DataInputStream(new FileInputStream(f2));
            String dictClassName2 = in.readUTF();
            dict2 = (Dictionary<String>) ClassUtil.newInstance(dictClassName2);
            dict2.readFields(in);
        }catch(IOException e){
            e.printStackTrace();
        }finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        assertTrue(dict.equals(dict2));
    }
}
