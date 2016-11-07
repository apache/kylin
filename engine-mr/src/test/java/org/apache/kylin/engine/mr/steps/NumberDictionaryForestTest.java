package org.apache.kylin.engine.mr.steps;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dict.NumberDictionary;
import org.apache.kylin.dict.NumberDictionaryBuilder;
import org.apache.kylin.dict.NumberDictionaryForest;
import org.apache.kylin.dict.NumberDictionaryForestBuilder;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionaryForestBuilder;
import org.apache.kylin.engine.mr.steps.fdc2.SelfDefineSortableKey;
import org.apache.kylin.engine.mr.steps.fdc2.TypeFlag;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by xiefan on 16-11-2.
 */


public class NumberDictionaryForestTest {
    @Test
    public void testNumberDictionaryForestLong(){
        List<String> list = randomLongData(10);
        testData(list,TypeFlag.INTEGER_FAMILY_TYPE);
    }

    @Test
    public void testNumberDictionaryForestDouble(){
        List<String> list = randomDoubleData(10);

        testData(list,TypeFlag.DOUBLE_FAMILY_TYPE);
    }

    private void testData(List<String> list,TypeFlag flag){
        //stimulate map-reduce job
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(list,(byte)flag.ordinal());
        Collections.sort(keyList);
        //build tree
        NumberDictionaryForestBuilder<String> b = new NumberDictionaryForestBuilder<String>(
                new StringBytesConverter(),0);
        TrieDictionaryForestBuilder.MaxTrieTreeSize = 0;
        for(SelfDefineSortableKey key : keyList){
            String fieldValue = printKey(key);
            b.addValue(fieldValue);
        }
        NumberDictionaryForest<String> dict = b.build();
        dict.dump(System.out);
        ArrayList<Integer> resultIds = new ArrayList<>();
        for(SelfDefineSortableKey key : keyList){
            String fieldValue = getFieldValue(key);
            resultIds.add(dict.getIdFromValue(fieldValue));
            assertEquals(fieldValue,dict.getValueFromId(dict.getIdFromValue(fieldValue)));
        }
        assertTrue(isIncreasedOrder(resultIds, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        }));
    }

    @Test
    public void serializeTest() {
        List<String> testData = new ArrayList<>();
        testData.add("1");
        testData.add("2");
        testData.add("100");
        //TrieDictionaryForestBuilder.MaxTrieTreeSize = 0;
        NumberDictionaryForestBuilder<String> b = new NumberDictionaryForestBuilder<String>(new StringBytesConverter());
        for(String str : testData)
            b.addValue(str);
        NumberDictionaryForest<String> dict = b.build();
        dict = testSerialize(dict);
        dict.dump(System.out);
        for (String str : testData) {
            assertEquals(str, dict.getValueFromId(dict.getIdFromValue(str)));
        }
    }

    @Test
    public void testVerySmallDouble(){
        List<String> testData = new ArrayList<>();
        testData.add(-1.0+"");
        testData.add(Double.MIN_VALUE+"");
        testData.add("1.01");
        testData.add("2.0");
        NumberDictionaryForestBuilder<String> b = new NumberDictionaryForestBuilder<String>(new StringBytesConverter());
        for(String str : testData)
            b.addValue(str);
        NumberDictionaryForest<String> dict = b.build();
        dict.dump(System.out);

        NumberDictionaryBuilder<String> b2 = new NumberDictionaryBuilder<>(new StringBytesConverter());
        for(String str : testData)
            b2.addValue(str);
        NumberDictionary<String> dict2 = b2.build(0);
        dict2.dump(System.out);

    }

    private static NumberDictionaryForest<String> testSerialize(NumberDictionaryForest<String> dict) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dataout = new DataOutputStream(bout);
            dict.write(dataout);
            dataout.close();
            ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
            DataInputStream datain = new DataInputStream(bin);
            NumberDictionaryForest<String> r = new NumberDictionaryForest<>();
            //r.dump(System.out);
            r.readFields(datain);
            //r.dump(System.out);
            datain.close();
            return r;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> randomLongData(int count){
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<String> list = new ArrayList<>();
        for(int i=0;i<count;i++){
            list.add(rand.nextLong()+"");
        }
        list.add(Long.MAX_VALUE+"");
        list.add(Long.MIN_VALUE+"");
        return list;
    }

    private List<String> randomDoubleData(int count){
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<String> list = new ArrayList<>();
        for(int i=0;i<count;i++){
            list.add(rand.nextDouble()+"");
        }
        list.add("-1");
        return list;
    }

    private List<String> randomStringData(int count){
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<String> list = new ArrayList<>();
        for(int i=0;i<count;i++){
            list.add(UUID.randomUUID().toString());
        }
        list.add("123");
        list.add("123");
        return list;
    }

    private ArrayList<SelfDefineSortableKey> createKeyList(List<String> strNumList,byte typeFlag){
        int partationId = 0;
        ArrayList<SelfDefineSortableKey> keyList = new ArrayList<>();
        for(String str : strNumList){
            ByteBuffer keyBuffer = ByteBuffer.allocate(4096);
            int offset = keyBuffer.position();
            keyBuffer.put(Bytes.toBytes(partationId)[3]);
            keyBuffer.put(Bytes.toBytes(str));
            //System.out.println(Arrays.toString(keyBuffer.array()));
            byte[] valueField = Bytes.copy(keyBuffer.array(),1,keyBuffer.position()-offset-1);
            //System.out.println("new string:"+new String(valueField));
            //System.out.println("arrays toString:"+Arrays.toString(valueField));
            Text outputKey = new Text();
            outputKey.set(keyBuffer.array(),offset,keyBuffer.position()-offset);
            SelfDefineSortableKey sortableKey = new SelfDefineSortableKey(typeFlag,outputKey);
            keyList.add(sortableKey);
        }
        return keyList;
    }

    private String printKey(SelfDefineSortableKey key){
        byte[] data = key.getText().getBytes();
        byte[] fieldValue = Bytes.copy(data,1,data.length-1);
        System.out.println("type flag:"+key.getTypeId()+" fieldValue:"+new String(fieldValue));
        return new String(fieldValue);
    }

    private String getFieldValue(SelfDefineSortableKey key){
        byte[] data = key.getText().getBytes();
        byte[] fieldValue = Bytes.copy(data,1,data.length-1);
        return new String(fieldValue);
    }

    private<T> boolean isIncreasedOrder(List<T> list, Comparator<T> comp){
        int flag;
        T previous = null;
        for(T t : list){
            if(previous == null) previous = t;
            else{
                flag = comp.compare(previous,t);
                if(flag > 0) return false;
                previous = t;
            }
        }
        return true;
    }
}
