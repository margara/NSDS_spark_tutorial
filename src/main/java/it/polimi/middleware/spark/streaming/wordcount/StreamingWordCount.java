package it.polimi.middleware.spark.streaming.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import it.polimi.middleware.spark.utils.LogUtils;
import scala.Tuple2;

public class StreamingWordCount {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String socketHost = args.length > 1 ? args[1] : "localhost";
        final int socketPort = args.length > 2 ? Integer.parseInt(args[2]) : 9999;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("StreamingWordCountSum");
        final JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

        final JavaPairDStream<String, Integer> counts = sc.socketTextStream(socketHost, socketPort)
                .window(Durations.seconds(10), Durations.seconds(5))
                .map(String::toLowerCase)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b);

        counts.foreachRDD(rdd -> rdd
                .collect()
                .forEach(System.out::println)
        );

        sc.start();

        try {
            sc.awaitTermination();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
        sc.close();
    }
}