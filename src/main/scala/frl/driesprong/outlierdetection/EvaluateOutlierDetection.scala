package frl.driesprong.outlierdetection

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat_ws, lit, max, min}

object EvaluateOutlierDetection {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Stochastic Outlier Selection")
      .getOrCreate()

    var df = spark.read.option("header", "true").csv("data/cardataset.csv")

    val vector_columns = Array("Engine HP", "Engine Cylinders", "highway MPG", "city mpg", "MSRP")


    vector_columns.foreach { col =>
      df = df.withColumn(col, df(col).cast("Double"))
      val minValue = lit(df.select(min(df(col))).first()(0))
      val maxValue = lit(df.select(max(df(col))).first()(0))
      println("Col " + col + " min " + minValue + ", max: " + maxValue)
      df = df.withColumn(col, (df(col) - minValue) / (maxValue - minValue))
    }

    val ass = new VectorAssembler().setInputCols(vector_columns).setOutputCol("vector")

    df = df.withColumn("label", concat_ws(" ", df("Make"), df("Model"), df("Year"), df("Engine Fuel Type"), df("Transmission Type")))

    df.count()

    df = ass.setHandleInvalid("skip").transform(df)
    df.count()

    val output = org.apache.spark.ml.outlierdetection.StochasticOutlierDetection.performOutlierDetectionDf(df)

    output.collect()
  }
}
