package com.wolf.test.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Description:
 * <br/> Created on 2017/12/6 14:44
 *
 * @author 李超
 * @since 1.0.0
 */
public class HelloSpark {

    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir","D:\\spark-2.2.0-bin-hadoop2.7");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("HelloSpark");//.setSparkHome("D:\\spark-2.1.0-bin-hadoop2.6");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        System.out.println(jsc);

        JavaRDD<String> inputRDD = jsc.textFile("D:\\data.txt");
        printRDD(inputRDD);
        filter(inputRDD);

        flatMap(jsc);

        union(jsc);

        groupByKey(jsc);

        reduceByKey(jsc);

        mapValues(jsc);

        join(jsc);

        count(jsc);

        saveAsTextFile(jsc);

        JavaRDD<Integer> distData = getRDDFromList(jsc);

        //所有数相加
        Integer reduce = distData.reduce((a, b) -> a + b);
        System.out.println("reduce==>" + reduce);

        mapReduce(jsc);


        wordcount(jsc);


    }

    private static void saveAsTextFile(JavaSparkContext jsc) {
        String[] strings = {"a", "b"};
        JavaRDD rdd4 = jsc.parallelize(toList(strings));
        rdd4.saveAsTextFile("D:\\qq.data");//part-00000中
    }

    private static void mapReduce(JavaSparkContext jsc) {
        //按照元素长度组成新rdd
        String[] strings = {"a", "bc", "def", "xet"};
        JavaRDD rdd = jsc.parallelize(toList(strings));
        JavaRDD result = rdd.map(
                new Function<String, Integer>() {
                    @Override
                    public Integer call(String x) throws Exception {
                        return x.length();
                    }
                });
        System.out.println("StringUtils.join==>" + StringUtils.join(result.collect(), ","));
        //将rdd中数据进行相加
        Object reduce2 = result.reduce(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer x, Integer y) throws Exception {
                        return x + y;
                    }
                });
        System.out.println("result2==>" + reduce2);
    }

    private static void wordcount(JavaSparkContext jsc) {
        //按行统计显示词频
        JavaRDD<String> file = jsc.textFile("D:\\wordcount.txt");
        //每一行构造一个Tuple2
        JavaPairRDD<String, Integer> paris = file.mapToPair(line -> new Tuple2(line, 1));
        System.out.println("paris==>" + paris.collect());
        JavaPairRDD<String, Integer> counters = paris.reduceByKey((a, b) -> a + b);
        counters.sortByKey();
        List<Tuple2<String, Integer>> collect = counters.collect();//可以将结果表示成Java的数组
        System.out.println("collect2==>" + collect);
    }

    private static void count(JavaSparkContext jsc) {
        String[] strings = {"a", "b"};
        JavaRDD rdd4 = jsc.parallelize(toList(strings));
        System.out.println("count==>"+rdd4.count());
    }

    //只有相同的key才会被联合
    private static void join(JavaSparkContext jsc) {
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<String, Integer>("a", 1));
        list.add(new Tuple2<String, Integer>("b", 1));
        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(list);

        List<Tuple2<String, Integer>> list2 = new ArrayList<>();
        list2.add(new Tuple2<String, Integer>("c", 1));
        list2.add(new Tuple2<String, Integer>("a", 3));
        JavaPairRDD<String, Integer> pairRDD2 = jsc.parallelizePairs(list2);

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = pairRDD.join(pairRDD2);
        System.out.println("join==>" + join.collect());
    }

    //操作value
    private static void mapValues(JavaSparkContext jsc) {
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<String, Integer>("a", 1));
        list.add(new Tuple2<String, Integer>("b", 1));
        list.add(new Tuple2<String, Integer>("c", 1));
        list.add(new Tuple2<String, Integer>("a", 3));
        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(list);
        JavaPairRDD<String, Integer> mapValues = pairRDD.mapValues(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1+4;
            }
        });
        System.out.println("mapValues==>" + mapValues.collect());
    }

    private static void reduceByKey(JavaSparkContext jsc) {
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<String, Integer>("a", 1));
        list.add(new Tuple2<String, Integer>("b", 1));
        list.add(new Tuple2<String, Integer>("c", 1));
        list.add(new Tuple2<String, Integer>("a", 3));
        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(list);
        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                //仅相同的分组才会调用此方法
                System.out.println("v1:" + v1 + " v2:" + v2);
                return v1 + v2;
            }
        });
        System.out.println("reduceByKey==>" + reduceByKey.collect());
    }

    private static void groupByKey(JavaSparkContext jsc) {
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<String, Integer>("a", 1));
        list.add(new Tuple2<String, Integer>("b", 1));
        list.add(new Tuple2<String, Integer>("c", 1));
        list.add(new Tuple2<String, Integer>("a", 3));
        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<Integer>> group = pairRDD.groupByKey();
        System.out.println("groupByKey==>" + group.collect());
    }

    private static void union(JavaSparkContext jsc) {
        String s = "a,b";
        JavaRDD rdd4 = jsc.parallelize(toList(s));
        String s1 = "c,d";
        JavaRDD rdd5 = jsc.parallelize(toList(s1));
        JavaRDD union = rdd4.union(rdd5);
        System.out.println("union==>" + union.collect());
    }

    private static void flatMap(JavaSparkContext jsc) {
        String[] strings = {"a,b", "c,d"};
        JavaRDD rdd3 = jsc.parallelize(toList(strings));
        JavaRDD result3 = rdd3.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String x) throws Exception {
                        return toList(x.split(",")).iterator();
                    }
                });
        System.out.println("flatMap==>" + StringUtils.join(result3.collect(), ","));
    }

    private static JavaRDD<Integer> getRDDFromList(JavaSparkContext jsc) {
        Integer[] integers = {1, 2, 3, 4, 5};
        List<Integer> data = toList(integers);
        JavaRDD<Integer> distData = jsc.parallelize(data);//作用在现有的集合上。该集合会被拷贝到别的节点上，以供并行操作。
        System.out.println("collect==>" + distData.collect());
        return distData;
    }

    //过滤
    private static void filter(JavaRDD<String> inputRDD) {
        JavaRDD errorsRDD = inputRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String x) throws Exception {
                        return x.contains("wor");
                    }
                });
        System.out.println("rdd filter collect==>" + errorsRDD.collect());
    }

    //打印
    private static void printRDD(JavaRDD<String> inputRDD) {

        inputRDD.foreach(new VoidFunction<String>() {
            public void call(String string) throws Exception {
                System.out.println(string);
            }
        });
    }

    public static <T> ArrayList<T> toList(T ...arr) {
        ArrayList<T> result = new ArrayList<>(arr.length);
        Collections.addAll(result, arr);
        return result;
    }
}
