package it.polimi.middleware.spark.batch.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountModified {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt");

        // Q1. For each character, compute the number of words starting with that character
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        List<Tuple2<String, Integer>> q1 = words
                .map(String::toLowerCase)
                .mapToPair(s -> new Tuple2<>(s.substring(0, 1), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();
        System.out.println("Results of query Q1");
        System.out.println(q1);

        // Q2. For each character, compute the number of lines starting with that character
        List<Tuple2<String, Integer>> q2 = lines
                .map(String::toLowerCase)
                .mapToPair(s -> new Tuple2<>(s.substring(0, 1), 1))
                .reduceByKey((a, b) -> a + b)
                .collect();
        System.out.println("Results of query Q2");
        System.out.println(q2);

        // Q3. Compute the average number of characters in each line
        Tuple2<Integer, Integer> q3 = lines
                .mapToPair(s -> new Tuple2<>(s.length(), 1))
                .reduce((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
        System.out.println("Results of query Q3");
        System.out.println(((float) q3._1) / q3._2);

        sc.close();
    }

}