package it.polimi.middleware.spark.streaming.wordcount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import it.polimi.middleware.spark.utils.LogUtils;
import scala.Tuple2;

public class StreamingWordCountWithState {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String socketHost = args.length > 1 ? args[1] : "localhost";
        final int socketPort = args.length > 2 ? Integer.parseInt(args[2]) : 9999;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("StreamingWordCountWithState");
        final JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Checkpoint directory where the state is stored
        sc.checkpoint("/tmp/");

        final Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mapFunction = //
                (word, count, state) -> {
                    final int sum = count.orElse(0) + (state.exists() ? state.get() : 0);
                    state.update(sum);
                    return new Tuple2<>(word, sum);
                };

        final List<Tuple2<String, Integer>> initialList = new ArrayList<>();
        final JavaPairRDD<String, Integer> initialRDD = sc.sparkContext().parallelizePairs(initialList);

        final JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> state = sc
                .socketTextStream(socketHost, socketPort)
                // .window(Durations.seconds(5))
                .map(String::toLowerCase)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .mapWithState(StateSpec.function(mapFunction).initialState(initialRDD));

        state.foreachRDD(rdd -> rdd
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
