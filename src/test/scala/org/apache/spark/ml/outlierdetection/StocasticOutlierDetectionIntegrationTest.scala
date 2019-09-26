package org.apache.spark.ml.outlierdetection

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest._

class StocasticOutlierDetectionIntegrationTest extends FlatSpec with Matchers with BeforeAndAfter {

  "Running the SOS algorithm " should "give some sensible outcome" in {

    val partitions = 22

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.allowMultipleContexts", value = true)
      .getOrCreate()

    var df = spark.read.option("header", "true").csv("data/cardataset.csv")

    val metricColumns = Array("Engine HP", "Engine Cylinders", "highway MPG", "city mpg", "Popularity", "MSRP")

    metricColumns.foreach { col =>
      df = df.withColumn(col, df(col).cast("Double"))
      val minValue = lit(df.select(min(df(col))).first()(0))
      val maxValue = lit(df.select(max(df(col))).first()(0))
      println("Col " + col + " min " + minValue + ", max: " + maxValue)
      df = df.withColumn(col, (df(col) - minValue) / (maxValue - minValue))
    }

    val ass = new VectorAssembler().setInputCols(metricColumns).setOutputCol("vector")

    df = df.withColumn("label", concat_ws(" ", df("Make"), df("Model"), df("Year"), df("Engine Fuel Type"), df("Transmission Type")))

    df = ass.setHandleInvalid("skip").transform(df)

    val num = df.count()

    val output = StochasticOutlierDetection.performOutlierDetectionDf(df.repartition(partitions), perplexity = Math.sqrt(num))

    val result = spark.createDataFrame(output).toDF("label", "score").cache()

//    println(result.show())
//    Thread.sleep(2200000)

    spark.stop()
  }

}
