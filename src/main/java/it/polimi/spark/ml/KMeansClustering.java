/*
 * Taken from the official Spark repository and modified.
 */

package it.polimi.spark.ml;

import it.polimi.spark.common.Consts;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KMeansClustering {

  public static void main(String[] args) {
    final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
    final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;

    final SparkSession spark = SparkSession
            .builder()
            .master(master)
            .appName("KMeans")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    // Load the data stored in LIBSVM format as a DataFrame.
    Dataset<Row> data = spark
            .read()
            .format("libsvm")
            .load(filePath + "files/ml/sample_libsvm_data.txt");

    // Trains a k-means model.
    KMeans kmeans = new KMeans().setK(2).setSeed(1L);
    KMeansModel model = kmeans.fit(data);

    // Make predictions
    Dataset<Row> predictions = model.transform(data);

    // Evaluate clustering by computing Silhouette score
    ClusteringEvaluator evaluator = new ClusteringEvaluator();

    double silhouette = evaluator.evaluate(predictions);
    System.out.println("Silhouette with squared euclidean distance = " + silhouette);

    // Shows the result.
    Vector[] centers = model.clusterCenters();
    System.out.println("Cluster Centers: ");
    for (Vector center : centers) {
      System.out.println(center);
    }

    spark.stop();
  }
}
