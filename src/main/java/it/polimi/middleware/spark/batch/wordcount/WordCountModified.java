package it.polimi.middleware.spark.batch.wordcount;

import it.polimi.middleware.spark.utils.LogUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountModified {

    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt");

        // Q1. For each character, compute the number of words starting with that character

        // TODO

        // Q2. For each character, compute the number of lines starting with that character

        // TODO

        // Q3. Compute the average number of characters in each line

        // TODO

        sc.close();
    }

}