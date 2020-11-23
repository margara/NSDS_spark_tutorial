package it.polimi.middleware.spark.streaming.event_time;

import it.polimi.middleware.spark.utils.LogUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.*;
import java.util.concurrent.TimeoutException;

public class WindowedCount {
    public static void main(String[] args) throws TimeoutException {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("WindowedCount")
                .getOrCreate();

        // Create DataFrame from a rate source.
        // A rate source generates records with a timestamp and a value at a fixed rate.
        // It is used for testing and benchmarking.
        final Dataset<Row> inputRecords = spark
                .readStream()
                .format("rate")
                .option("rowPerSecond", 10)
                .load();

        inputRecords.withWatermark("timestamp", "1 hour");

        // Group by window and by value (try to only group by one of the two)
        final StreamingQuery query = inputRecords
            .groupBy(
                window(col("timestamp"), "30 seconds", "10 seconds"),
                    col("value")
            )
                .count()
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }

}