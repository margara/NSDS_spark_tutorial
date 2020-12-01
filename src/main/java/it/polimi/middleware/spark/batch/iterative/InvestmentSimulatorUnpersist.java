package it.polimi.middleware.spark.batch.iterative;

import it.polimi.middleware.spark.utils.LogUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Start from a dataset of investments. Each element is a Tuple2(amount_owned, interest_rate).
 * At each iteration the new amount is (amount_owned * (1+interest_rate)).
 *
 * Implement an iterative algorithm that computes the new amount for each investment and stops
 * when the overall amount overcomes 1000.
 *
 * Exemplifies the use of unpersist.
 */
public class InvestmentSimulatorUnpersist {
    private static int numMapInvocations = 0;

    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[1]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final double threshold = 1000;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("InvestmentSimulator");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> textFile = sc.textFile(filePath + "files/iterative/investment.txt");

        // Transforms each line into a tuple (amount_owned, investment_rate)
        JavaRDD<Tuple2<Double, Double>> investments = textFile.map(w -> {
            String[] values = w.split(" ");
            double amountOwned = Double.parseDouble(values[0]);
            double investmentRate = Double.parseDouble(values[1]);
            return new Tuple2(amountOwned, investmentRate);
        });

        JavaRDD<Tuple2<Double, Double>> oldInvestments = investments;
        investments.cache();

        int iteration = 0;
        double sum = sumAmount(investments);
        while (sum < threshold) {
            System.out.println("Iteration: " + (iteration++));
            investments = investments.map(i -> {
                System.out.println("Number of invocations of the map " + (numMapInvocations++));
                return new Tuple2<>(i._1*(1+i._2), i._2);
            });
            investments.cache();
            sum = sumAmount(investments);

            // Important: this needs to be done after the action (sumAmount)
            // otherwise the old investments would be unpersisted before the
            // new investments are computed and cached.
            // Try to move it before the action and see how the prints change.
            oldInvestments.unpersist();
            oldInvestments = investments;
        }

        System.out.println("Sum: " + sum + " after " + iteration + " iterations");
        sc.close();
    }

    private static final double sumAmount(JavaRDD<Tuple2<Double, Double>> investments) {
        return investments
                .map(a -> a._1)
                .reduce((a, b) -> a+b);
    }

}
