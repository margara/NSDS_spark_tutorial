package it.polimi.spark.batch.wordcount;

import it.polimi.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCountModified {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

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