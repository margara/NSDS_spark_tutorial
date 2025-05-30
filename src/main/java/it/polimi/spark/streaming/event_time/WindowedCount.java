package it.polimi.spark.streaming.event_time;

import it.polimi.spark.common.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.*;

public class WindowedCount {
    public static void main(String[] args) throws Exception {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("WindowedCount")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // Create DataFrame from a rate source.
        // A rate source generates records with a timestamp and a value at a fixed rate.
        // It is used for testing and benchmarking.
        final Dataset<Row> inputRecords = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 10)
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