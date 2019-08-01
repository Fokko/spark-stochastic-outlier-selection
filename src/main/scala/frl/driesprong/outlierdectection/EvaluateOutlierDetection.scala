package frl.driesprong.outlierdectection

import org.apache.spark.{SparkConf, SparkContext}

object EvaluateOutlierDetection {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Stochastic Outlier Selection")

    val sc = new SparkContext(conf)

    val toyDataset = Array(
      (0L, Array(1.00, 1.00)),
      (1L, Array(3.00, 1.25)),
      (2L, Array(3.00, 3.00)),
      (3L, Array(1.00, 3.00)),
      (4L, Array(2.25, 2.25)),
      (5L, Array(8.00, 2.00))
    )

    StochasticOutlierDetection.performOutlierDetection( sc.parallelize(toyDataset) ).foreach( x =>
      System.out.println(x._1 + " : " + x._2)
    )

    sc.stop()
  }
}
