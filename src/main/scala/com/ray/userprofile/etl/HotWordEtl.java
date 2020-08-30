package com.ray.userprofile.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * WordCount实例
 * 热词提取（词云）
 * 所谓词云，其实就是统计关键词的频率。
 * 只要是用户评价、商品类别之类的文本信息，我们都可以按空格分词，然后统计每个词出现的次数——就相当于是一个word count，然后按照count数量降序排列就可以了。
 */
public class HotWordEtl {
    public static void main(String[] args) {
        // 为了方便操作数据，首先创建一个jsc：
        SparkConf sc = new SparkConf()
                .setAppName("hot word etl")
                .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        // 数据文件在hdfs上
        System.setProperty("HADOOP_USER_NAME", "root");

        // 用jsc读取hdfs文件，转成java rdd
//        JavaRDD<String> linesRdd = jsc.textFile("hdfs://192.168.0.8:9000/tmp/SogouQ.sample.txt");
        //因为cdh集群默认是8020端口，进行内部rpc通信
        JavaRDD<String> linesRdd = jsc.textFile("hdfs://192.168.0.8:8020/tmp/SogouQ.sample.txt");

        JavaPairRDD<String, Integer> pairRDD = linesRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                // 以制表符分隔，取第三个字段
//                String word = s.split("\t")[2];
                // 以制表符分隔，取第一个字段
                String word = s.split("\t")[0];
                return new Tuple2<>(word, 1);
            }
        });

        // 以word作为key，分组聚合
        JavaPairRDD<String, Integer> countRdd = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 元素互换位置
        JavaPairRDD<Integer, String> swapedRdd = countRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        // 按照count排序
        JavaPairRDD<Integer, String> sortedRdd = swapedRdd.sortByKey(false);

        // 再互换位置
        JavaPairRDD<String, Integer> resultRdd = sortedRdd.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        // 取前10个热词输出
        List<Tuple2<String, Integer>> hotWordCounts = resultRdd.take(10);

        // 打印输出
        for (Tuple2<String, Integer> hotWordCount : hotWordCounts) {
            System.out.println(hotWordCount._1 + " === count " + hotWordCount._2);
        }

        /**
         * 结果：
         * [婚姻法] === count 6
         * [歌曲爱的奉献] === count 6
         * [雅芳化妆品] === count 6
         * [电脑桌面图片下载] === count 6
         * [断点] === count 5
         * [瑞丽服饰] === count 5
         * [职业规划] === count 5
         * [天亮以后不分手] === count 5
         * [财经网] === count 5
         * [qq医生下载] === count 5
         */
    }
}
