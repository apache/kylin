package org.apache.kylin.engine.mr.steps;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dict.NumberDictionaryForest;
import org.apache.kylin.dict.NumberDictionaryForestBuilder;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.dict.TrieDictionaryBuilder;
import org.apache.kylin.dict.TrieDictionaryForest;
import org.apache.kylin.dict.TrieDictionaryForestBuilder;
import org.apache.kylin.engine.mr.steps.fdc2.SelfDefineSortableKey;
import org.apache.kylin.engine.mr.steps.fdc2.TypeFlag;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Created by xiefan on 16-11-2.
 */
public class SelfDefineSortableKeyTest {

    @Test
    public void testSortLong(){
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<Long> longList = new ArrayList<>();
        int count = 10;
        for(int i=0;i<count;i++){
            longList.add(rand.nextLong());
        }
        longList.add(0L);
        longList.add(0L); //test duplicate
        longList.add(-1L); //test negative number
        longList.add(Long.MAX_VALUE);
        longList.add(Long.MIN_VALUE);

        System.out.println("test numbers:"+longList);
        ArrayList<String> strNumList = listToStringList(longList);
        //System.out.println("test num strs list:"+strNumList);
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strNumList, (byte)TypeFlag.INTEGER_FAMILY_TYPE.ordinal());
        System.out.println(keyList.get(0).isIntegerFamily());
        Collections.sort(keyList);
        ArrayList<String> strListAftereSort = new ArrayList<>();
        for(SelfDefineSortableKey key : keyList){
            String str = printKey(key);
            strListAftereSort.add(str);
        }
        assertTrue(isIncreasedOrder(strListAftereSort, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                Long l1 = Long.parseLong(o1);
                Long l2 = Long.parseLong(o2);
                return l1.compareTo(l2);
            }
        }));
    }

    @Test
    public void testSortDouble(){
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<Double> doubleList = new ArrayList<>();
        int count = 10;
        for(int i=0;i<count;i++){
            doubleList.add(rand.nextDouble());
        }
        doubleList.add(0.0);
        doubleList.add(0.0); //test duplicate
        doubleList.add(-1.0); //test negative number
        doubleList.add(Double.MAX_VALUE);
        doubleList.add(-Double.MAX_VALUE);
        //System.out.println(Double.MIN_VALUE);

        System.out.println("test numbers:"+doubleList);
        ArrayList<String> strNumList = listToStringList(doubleList);
        //System.out.println("test num strs list:"+strNumList);
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strNumList, (byte)TypeFlag.DOUBLE_FAMILY_TYPE.ordinal());
        System.out.println(keyList.get(0).isOtherNumericFamily());
        Collections.sort(keyList);
        ArrayList<String> strListAftereSort = new ArrayList<>();
        for(SelfDefineSortableKey key : keyList){
            String str = printKey(key);
            strListAftereSort.add(str);
        }
        assertTrue(isIncreasedOrder(strListAftereSort, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                Double d1 = Double.parseDouble(o1);
                Double d2 = Double.parseDouble(o2);
                return d1.compareTo(d2);
            }
        }));
    }

    @Test
    public void testSortNormalString(){
        int count = 10;
        ArrayList<String> strList = new ArrayList<>();
        for(int i=0;i<count;i++){
            UUID uuid = UUID.randomUUID();
            strList.add(uuid.toString());
        }
        strList.add("hello");
        strList.add("hello"); //duplicate
        strList.add("123");
        strList.add("");
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strList, (byte)TypeFlag.NONE_NUMERIC_TYPE.ordinal());
        System.out.println(keyList.get(0).isOtherNumericFamily());
        Collections.sort(keyList);
        ArrayList<String> strListAftereSort = new ArrayList<>();
        for(SelfDefineSortableKey key : keyList){
            String str = printKey(key);
            strListAftereSort.add(str);
        }
        assertTrue(isIncreasedOrder(strListAftereSort, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
               return o1.compareTo(o2);
            }
        }));
    }

    @Test
    public void testIllegalNumber(){
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<Double> doubleList = new ArrayList<>();
        int count = 10;
        for(int i=0;i<count;i++){
            doubleList.add(rand.nextDouble());
        }
        doubleList.add(0.0);
        doubleList.add(0.0); //test duplicate
        doubleList.add(-1.0); //test negative number
        doubleList.add(Double.MAX_VALUE);
        doubleList.add(-Double.MAX_VALUE);
        //System.out.println(Double.MIN_VALUE);

        System.out.println("test numbers:"+doubleList);
        ArrayList<String> strNumList = listToStringList(doubleList);
        strNumList.add("fjaeif"); //illegal type
        //System.out.println("test num strs list:"+strNumList);
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strNumList, (byte)TypeFlag.DOUBLE_FAMILY_TYPE.ordinal());
        System.out.println(keyList.get(0).isOtherNumericFamily());
        Collections.sort(keyList);
        for(SelfDefineSortableKey key : keyList){
            printKey(key);
        }

    }

    @Test
    public void testEnum(){
        TypeFlag flag = TypeFlag.DOUBLE_FAMILY_TYPE;
        System.out.println((byte)flag.ordinal());
        int t = (byte)flag.ordinal();
        System.out.println(t);
    }



    private<T> ArrayList<String> listToStringList(ArrayList<T> list){
        ArrayList<String> strList = new ArrayList<>();
        for(T t : list){
            System.out.println(t.toString());
            strList.add(t.toString());
        }
        return strList;
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
