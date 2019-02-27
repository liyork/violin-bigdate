package com.wolf.test.hadoop;

import org.apache.hadoop.io.*;

import java.util.Set;

/**
 * Description:
 * <br/> Created on 12/28/17 8:02 PM
 *
 * @author 李超
 * @since 1.0.0
 */
public class WritablesTest {

    public static class IntArrayWritable extends ArrayWritable{

        public IntArrayWritable(Class<? extends Writable> valueClass) {
            super(IntWritable.class);
        }
    }

    public static void main(String[] args) {
        BooleanWritable booleanWritable = new BooleanWritable(true);
        ByteWritable byteWritable = new ByteWritable((byte)3);
        System.out.printf("Boolean:%s Byte:%d\n",booleanWritable,byteWritable.get());

        IntWritable intWritable = new IntWritable(5);
        System.out.printf("Int:%d\n",intWritable.get());
        intWritable.set(44);
        System.out.printf("Int2:%d\n",intWritable.get());
        Integer i3 = new Integer(23);
        intWritable.set(i3);
        System.out.printf("Int3:%d\n",intWritable.get());

        ArrayWritable arrayWritable = new ArrayWritable(IntWritable.class);
        arrayWritable.set(new IntWritable[]{new IntWritable(1),new IntWritable(3)});
        IntWritable[] writables = (IntWritable[])arrayWritable.get();
        for (Writable writable : writables) {
            System.out.println(writable);
        }

        IntArrayWritable intArrayWritable = new IntArrayWritable(IntWritable.class);
        intArrayWritable.set(new IntWritable[]{new IntWritable(1),new IntWritable(3)});
        intArrayWritable.set(new LongWritable[]{new LongWritable(100L)});//由于没有泛型支持，要小心

        MapWritable mapWritable = new MapWritable();
        IntWritable intWritable1 =  new IntWritable(5);
        NullWritable nullWritable = NullWritable.get();
        mapWritable.put(intWritable1,nullWritable);
        System.out.println(mapWritable.containsKey(intWritable1));
        System.out.println(mapWritable.get(intWritable1));
        mapWritable.put(new LongWritable(2000L), intWritable1);
        Set<Writable> writables1 = mapWritable.keySet();
        for (Writable writable : writables1) {
            System.out.println(writable.getClass());
        }

    }
}
