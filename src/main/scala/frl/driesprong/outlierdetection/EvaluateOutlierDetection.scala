package frl.driesprong.outlierdetection

import org.apache.spark.{SparkConf, SparkContext}

object EvaluateOutlierDetection {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Stochastic Outlier Selection")

    val sc = new SparkContext(conf)

    val toyDataset = Array(
      Array(1.00, 1.00),
      Array(3.00, 1.25),
      Array(3.00, 3.00),
      Array(1.00, 3.00),
      Array(2.25, 2.25),
      Array(8.00, 2.00)
    )

    val rdd = sc.parallelize(toyDataset)

    StochasticOutlierDetection.performOutlierDetection(rdd).foreach(x =>
      System.out.println(x._1 + " : " + x._2)
    )

    sc.stop()
  }
}
