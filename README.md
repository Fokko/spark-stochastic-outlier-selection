[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build Status](https://travis-ci.org/Fokko/spark-stochastic-outlier-selection.svg?branch=master)](https://travis-ci.org/Fokko/spark-stochastic-outlier-selection)
[![Coverage Status](https://coveralls.io/repos/Fokko/spark-stochastic-outlier-selection/badge.svg?branch=master&service=github)](https://coveralls.io/github/Fokko/spark-stochastic-outlier-selection?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/frl.driesprong/spark-stochastic-outlier-selection_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/frl.driesprong/spark-stochastic-outlier-selection_2.11)

# Stochastic Outlier Selection on Apache Spark

Stochastic Outlier Selection (SOS) is an unsupervised outlier selection algorithm. It uses the concept of affinity to compute an outlier probability for each data point.

For more information about SOS, see the technical report: J.H.M. Janssens, F. Huszar, E.O. Postma, and H.J. van den Herik. [Stochastic Outlier Selection](https://github.com/jeroenjanssens/sos/blob/master/doc/sos-ticc-tr-2012-001.pdf?raw=true). Technical Report TiCC TR 2012-001, Tilburg University, Tilburg, the Netherlands, 2012.

## Selecting outliers from data

The current implementation accepts RDD's of the type `Array[Double]` and returns the indexes of the vector with it's degree of outlierness.

Current implementation only works with Euclidean distance, but this can be extended.

# Example

As a small example on how to use the algorithm, we have the following dataset:

```
scala> val df = spark.read.option("header", "true").csv("data/cardataset.csv")
df: org.apache.spark.sql.DataFrame = [Make: string, Model: string ... 14 more fields]

scala> df.show()
+----+----------+----+--------------------+---------+----------------+-----------------+-----------------+---------------+--------------------+------------+-------------+-----------+--------+----------+-----+
|Make|     Model|Year|    Engine Fuel Type|Engine HP|Engine Cylinders|Transmission Type|    Driven_Wheels|Number of Doors|     Market Category|Vehicle Size|Vehicle Style|highway MPG|city mpg|Popularity| MSRP|
+----+----------+----+--------------------+---------+----------------+-----------------+-----------------+---------------+--------------------+------------+-------------+-----------+--------+----------+-----+
| BMW|1 Series M|2011|premium unleaded ...|      335|               6|           MANUAL| rear wheel drive|              2|Factory Tuner,Lux...|     Compact|        Coupe|         26|      19|      3916|46135|
| BMW|  1 Series|2011|premium unleaded ...|      300|               6|           MANUAL| rear wheel drive|              2|  Luxury,Performance|     Compact|  Convertible|         28|      19|      3916|40650|
| BMW|  1 Series|2011|premium unleaded ...|      300|               6|           MANUAL| rear wheel drive|              2|Luxury,High-Perfo...|     Compact|        Coupe|         28|      20|      3916|36350|
| BMW|  1 Series|2011|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|  Luxury,Performance|     Compact|        Coupe|         28|      18|      3916|29450|
| BMW|  1 Series|2011|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|              Luxury|     Compact|  Convertible|         28|      18|      3916|34500|
| BMW|  1 Series|2012|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|  Luxury,Performance|     Compact|        Coupe|         28|      18|      3916|31200|
| BMW|  1 Series|2012|premium unleaded ...|      300|               6|           MANUAL| rear wheel drive|              2|  Luxury,Performance|     Compact|  Convertible|         26|      17|      3916|44100|
| BMW|  1 Series|2012|premium unleaded ...|      300|               6|           MANUAL| rear wheel drive|              2|Luxury,High-Perfo...|     Compact|        Coupe|         28|      20|      3916|39300|
| BMW|  1 Series|2012|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|              Luxury|     Compact|  Convertible|         28|      18|      3916|36900|
| BMW|  1 Series|2013|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|              Luxury|     Compact|  Convertible|         27|      18|      3916|37200|
| BMW|  1 Series|2013|premium unleaded ...|      300|               6|           MANUAL| rear wheel drive|              2|Luxury,High-Perfo...|     Compact|        Coupe|         28|      20|      3916|39600|
| BMW|  1 Series|2013|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|  Luxury,Performance|     Compact|        Coupe|         28|      19|      3916|31500|
| BMW|  1 Series|2013|premium unleaded ...|      300|               6|           MANUAL| rear wheel drive|              2|  Luxury,Performance|     Compact|  Convertible|         28|      19|      3916|44400|
| BMW|  1 Series|2013|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|              Luxury|     Compact|  Convertible|         28|      19|      3916|37200|
| BMW|  1 Series|2013|premium unleaded ...|      230|               6|           MANUAL| rear wheel drive|              2|  Luxury,Performance|     Compact|        Coupe|         28|      19|      3916|31500|
| BMW|  1 Series|2013|premium unleaded ...|      320|               6|           MANUAL| rear wheel drive|              2|Luxury,High-Perfo...|     Compact|  Convertible|         25|      18|      3916|48250|
| BMW|  1 Series|2013|premium unleaded ...|      320|               6|           MANUAL| rear wheel drive|              2|Luxury,High-Perfo...|     Compact|        Coupe|         28|      20|      3916|43550|
|Audi|       100|1992|    regular unleaded|      172|               6|           MANUAL|front wheel drive|              4|              Luxury|     Midsize|        Sedan|         24|      17|      3105| 2000|
|Audi|       100|1992|    regular unleaded|      172|               6|           MANUAL|front wheel drive|              4|              Luxury|     Midsize|        Sedan|         24|      17|      3105| 2000|
|Audi|       100|1992|    regular unleaded|      172|               6|        AUTOMATIC|  all wheel drive|              4|              Luxury|     Midsize|        Wagon|         20|      16|      3105| 2000|
+----+----------+----+--------------------+---------+----------------+-----------------+-----------------+---------------+--------------------+------------+-------------+-----------+--------+----------+-----+
only showing top 20 rows
```

Borrowed from Kaggle: https://www.kaggle.com/CooperUnion/cardataset. The data is scraped from Edmunds, which serves the US of A, so we might expect some different cars compared to Europe or somewhere else.

The run might take some test since the algorithm is [quadratic in runtime](https://en.wikipedia.org/wiki/Big_O_notation). This means, for the example we insert 11816 rows, which boils down to a dense 11816 x 11816 distance matrix consisting of 139.617.856 doubles.

## SOS using Scala

```
MacBook-Pro-van-Fokko:spark-stochastic-outlier-selection fokkodriesprong$ spark-shell --jars target/scala-2.11/spark-stochastic-outlier-selection_2.11-0.1.0.jar 
19/09/24 12:22:53 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://192.168.185.146:4040
Spark context available as 'sc' (master = local[*], app id = local-1569320579133).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/
         
Using Scala version 2.11.12 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_172)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val partitions = 22
partitions: Int = 22

scala> var df = spark.read.option("header", "true").csv("data/cardataset.csv")
df: org.apache.spark.sql.DataFrame = [Make: string, Model: string ... 14 more fields]

scala> val metricColumns = Array("Engine HP", "Engine Cylinders", "highway MPG", "city mpg", "Popularity", "MSRP")
metricColumns: Array[String] = Array(Engine HP, Engine Cylinders, highway MPG, city mpg, Popularity, MSRP)

scala> metricColumns.foreach { col =>
     |   df = df.withColumn(col, df(col).cast("Double"))
     |   val minValue = lit(df.select(min(df(col))).first()(0))
     |   val maxValue = lit(df.select(max(df(col))).first()(0))
     |   println("Col " + col + " min " + minValue + ", max: " + maxValue)
     |   df = df.withColumn(col, (df(col) - minValue) / (maxValue - minValue))
     | }
Col Engine HP min 55.0, max: 1001.0
Col Engine Cylinders min 0.0, max: 16.0
Col highway MPG min 12.0, max: 354.0
Col city mpg min 7.0, max: 137.0
Col Popularity min 2.0, max: 5657.0
Col MSRP min 2000.0, max: 2065902.0

scala> import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorAssembler

scala> val ass = new VectorAssembler().setInputCols(metricColumns).setOutputCol("vector")
ass: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_20553ab89e72

scala> df = df.withColumn("label", concat_ws(" ", df("Make"), df("Model"), df("Year"), df("Engine Fuel Type"), df("Transmission Type")))
df: org.apache.spark.sql.DataFrame = [Make: string, Model: string ... 15 more fields]

scala> df = ass.setHandleInvalid("skip").transform(df)
df: org.apache.spark.sql.DataFrame = [Make: string, Model: string ... 16 more fields]

scala> val num = df.count()
num: Long = 11816

scala> import org.apache.spark.ml.outlierdetection.StochasticOutlierDetection
import org.apache.spark.ml.outlierdetection.StochasticOutlierDetection

scala> val output = StochasticOutlierDetection.performOutlierDetectionDf(df.repartition(partitions), perplexity = Math.sqrt(num))
output: org.apache.spark.rdd.RDD[(String, Double)] = MapPartitionsRDD[124] at map at StochasticOutlierDetection.scala:68

scala> val result = spark.createDataFrame(output).toDF("label", "score").cache()
result: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [label: string, score: double]

scala> result.orderBy(asc("score")).show(22, false)
+---------------------------------------------------------------+-------------------+
|label                                                          |score              |
+---------------------------------------------------------------+-------------------+
|Chevrolet Sonic 2015 regular unleaded MANUAL                   |0.21279828599808828|
|Chevrolet Sonic 2016 regular unleaded MANUAL                   |0.21293609075531097|
|Chevrolet Sonic 2016 regular unleaded MANUAL                   |0.2131071436509756 |
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.213830724190521  |
|Chevrolet Sonic 2015 regular unleaded MANUAL                   |0.213858747973501  |
|GMC Sierra 1500 Classic 2007 regular unleaded AUTOMATIC        |0.21396034795422958|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21396720338838232|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.2140147473705729 |
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21406002357897783|
|GMC Sierra 1500 Classic 2007 regular unleaded AUTOMATIC        |0.21415665139143406|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21418025039928237|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21448192959828502|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21649415207000774|
|GMC Sierra 1500 2016 flex-fuel (unleaded/E85) AUTOMATIC        |0.21656724572039546|
|GMC Sierra 1500 2015 flex-fuel (unleaded/E85) AUTOMATIC        |0.21659862416390824|
|GMC Sierra 1500 2015 flex-fuel (unleaded/E85) AUTOMATIC        |0.21665169162695752|
|GMC Sierra 1500 2017 flex-fuel (unleaded/E85) AUTOMATIC        |0.21667668432518386|
|GMC Sierra 1500 2016 flex-fuel (unleaded/E85) AUTOMATIC        |0.2167653474971194 |
|Infiniti EX 2011 premium unleaded (recommended) AUTOMATIC      |0.21723682724336277|
|Infiniti EX 2012 premium unleaded (recommended) AUTOMATIC      |0.21725696468322728|
|GMC Sierra 1500 Classic 2007 regular unleaded AUTOMATIC        |0.21726098834132695|
|GMC Sierra 1500 2017 flex-fuel (unleaded/E85) AUTOMATIC        |0.2172733380874455 |
+---------------------------------------------------------------+-------------------+
only showing top 22 rows


scala> result.orderBy(desc("score")).show(22, false)
+----------------------------------------------------------------------+------------------+
|label                                                                 |score             |
+----------------------------------------------------------------------+------------------+
|Audi A6 2017 premium unleaded (recommended) AUTOMATED_MANUAL          |0.9999142633116896|
|Porsche 718 Cayman 2017 premium unleaded (required) MANUAL            |0.9805813377048511|
|Rolls-Royce Corniche 2001 premium unleaded (required) AUTOMATIC       |0.9799020737648789|
|Acura NSX 2017 premium unleaded (required) AUTOMATED_MANUAL           |0.9686265528700773|
|Volkswagen Touareg 2 2008 diesel AUTOMATIC                            |0.9620829288975413|
|Mitsubishi Mighty Max Pickup 1994 regular unleaded MANUAL             |0.9484213630955308|
|Chrysler Aspen 2009 regular unleaded AUTOMATIC                        |0.9271092903462673|
|Oldsmobile Cutlass Ciera 1994 regular unleaded AUTOMATIC              |0.9119041256238049|
|Volkswagen Touareg 2015 premium unleaded (recommended) AUTOMATIC      |0.9067141244279047|
|Chrysler TC 1990 regular unleaded MANUAL                              |0.8942706414341941|
|BMW 7 Series 2015 premium unleaded (required) AUTOMATIC               |0.887326636862905 |
|Ferrari Enzo 2003 premium unleaded (required) AUTOMATED_MANUAL        |0.876073945508002 |
|Audi 80 1990 regular unleaded MANUAL                                  |0.8712155642417887|
|Mitsubishi Vanwagon 1990 regular unleaded AUTOMATIC                   |0.8604594927230855|
|Lamborghini Reventon 2008 premium unleaded (required) AUTOMATED_MANUAL|0.8569533527940364|
|Saab 900 1996 regular unleaded MANUAL                                 |0.8517167561224662|
|Ford Focus RS 2017 premium unleaded (recommended) MANUAL              |0.8369213995663755|
|Hyundai Elantra 2017 regular unleaded AUTOMATED_MANUAL                |0.8145671662996115|
|Ford Focus 2017 regular unleaded MANUAL                               |0.8080970764431054|
|Mercedes-Benz E-Class 2015 premium unleaded (required) AUTOMATIC      |0.8045429849252246|
|BMW M4 GTS 2016 premium unleaded (required) AUTOMATED_MANUAL          |0.7994977767039853|
|Chevrolet Cruze 2015 diesel AUTOMATIC                                 |0.7935704861212711|
+----------------------------------------------------------------------+------------------+
only showing top 22 rows
```

## SOS using PySpark

```
MacBook-Pro-van-Fokko:spark-stochastic-outlier-selection fokkodriesprong$ pyspark --jars target/scala-2.11/spark-stochastic-outlier-selection_2.11-0.1.0.jar 
Python 3.6.6 (v3.6.6:4cf1f54eb7, Jun 26 2018, 19:50:54) 
[GCC 4.2.1 Compatible Apple LLVM 6.0 (clang-600.0.57)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
19/09/24 12:11:22 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Python version 3.6.6 (v3.6.6:4cf1f54eb7, Jun 26 2018 19:50:54)
SparkSession available as 'spark'.
>>> import math
>>> from pyspark.ml.feature import VectorAssembler
>>> from pyspark.sql.functions import col, lit, min, max, asc, desc, concat_ws
>>> from pyspark.mllib.common import _java2py
>>> 
>>> df = spark.read.option("header", "true").csv("data/cardataset.csv")
>>> 
>>> metric_columns = ["Engine HP", "Engine Cylinders", "highway MPG", "city mpg", "Popularity", "MSRP"]
>>> 
>>> # Scale the columns and remove the empty ones
... for column_name in metric_columns:
...     df = df.withColumn(column_name, col(column_name).cast("Double"))
...     minValue = lit(df.select(min(col(column_name)).alias("min")).first().asDict()['min'])
...     maxValue = lit(df.select(max(col(column_name)).alias("max")).first().asDict()['max'])
...     print("Col " + column_name + " min " + str(minValue) + ", max: " + str(maxValue))
...     df = df.where(col(column_name).isNotNull())
...     df = df.withColumn(column_name, (col(column_name) - minValue) / (maxValue - minValue))
... 
Col Engine HP min Column<b'55.0'>, max: Column<b'1001.0'>
Col Engine Cylinders min Column<b'0.0'>, max: Column<b'16.0'>
Col highway MPG min Column<b'12.0'>, max: Column<b'354.0'>
Col city mpg min Column<b'7.0'>, max: Column<b'137.0'>
Col Popularity min Column<b'2.0'>, max: Column<b'5657.0'>
Col MSRP min Column<b'2000.0'>, max: Column<b'2065902.0'>
>>> 
>>> df = df.withColumn("label", concat_ws(" ", col("Make"), col("Model"), col("Year"), col("Engine Fuel Type"), col("Transmission Type")))
>>> 
>>> # Count the number of rows
... num = df.count()
>>> num
11816
>>> 
>>> # Remove the missing vectors and combine all the columns to a single vector
... ass = VectorAssembler(inputCols=metric_columns, outputCol="vector")
>>> df = ass.setHandleInvalid("skip").transform(df).repartition(22)
>>> 
>>> # As perplexity, use the sqrt of the number of rows
... perplexity = math.sqrt(num)
>>> 
>>> jvm = sc._jvm
>>> sqlContext = df._jdf
>>> 
>>> sos = jvm.org.apache.spark.ml.outlierdetection.StochasticOutlierDetection.performOutlierDetectionPython(spark._jwrapped, df._jdf, "label", "vector", perplexity, 1e-9, 5000)
>>> 
>>> # Reconstruct the Python DF
... result_df = _java2py(sc, sos)
>>> 
>>> result_df.orderBy(asc("score")).show(22, False)
+---------------------------------------------------------------+-------------------+
|label                                                          |score              |
+---------------------------------------------------------------+-------------------+
|Chevrolet Sonic 2015 regular unleaded MANUAL                   |0.2127982860017602 |
|Chevrolet Sonic 2016 regular unleaded MANUAL                   |0.21293609075267098|
|Chevrolet Sonic 2016 regular unleaded MANUAL                   |0.21310714365721425|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21383072418134913|
|Chevrolet Sonic 2015 regular unleaded MANUAL                   |0.21385874797566645|
|GMC Sierra 1500 Classic 2007 regular unleaded AUTOMATIC        |0.21396034794647772|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21396720338004566|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.2140147473677278 |
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.2140600235686727 |
|GMC Sierra 1500 Classic 2007 regular unleaded AUTOMATIC        |0.21415665138439838|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21418025039550595|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21448192959623724|
|GMC Sierra 1500 Classic 2007 flex-fuel (unleaded/E85) AUTOMATIC|0.21649415206093467|
|GMC Sierra 1500 2016 flex-fuel (unleaded/E85) AUTOMATIC        |0.21656724570838756|
|GMC Sierra 1500 2015 flex-fuel (unleaded/E85) AUTOMATIC        |0.21659862415217437|
|GMC Sierra 1500 2015 flex-fuel (unleaded/E85) AUTOMATIC        |0.21665169161813994|
|GMC Sierra 1500 2017 flex-fuel (unleaded/E85) AUTOMATIC        |0.21667668431185894|
|GMC Sierra 1500 2016 flex-fuel (unleaded/E85) AUTOMATIC        |0.21676534748163712|
|Infiniti EX 2011 premium unleaded (recommended) AUTOMATIC      |0.2172368272372638 |
|Infiniti EX 2012 premium unleaded (recommended) AUTOMATIC      |0.21725696467803174|
|GMC Sierra 1500 Classic 2007 regular unleaded AUTOMATIC        |0.2172609883362982 |
|GMC Sierra 1500 2017 flex-fuel (unleaded/E85) AUTOMATIC        |0.21727333807551774|
+---------------------------------------------------------------+-------------------+
only showing top 22 rows

>>> result_df.orderBy(desc("score")).show(22, False)
+----------------------------------------------------------------------+------------------+
|label                                                                 |score             |
+----------------------------------------------------------------------+------------------+
|Audi A6 2017 premium unleaded (recommended) AUTOMATED_MANUAL          |0.9999142633116839|
|Porsche 718 Cayman 2017 premium unleaded (required) MANUAL            |0.980581337706238 |
|Rolls-Royce Corniche 2001 premium unleaded (required) AUTOMATIC       |0.9799020737640686|
|Acura NSX 2017 premium unleaded (required) AUTOMATED_MANUAL           |0.9686265528721012|
|Volkswagen Touareg 2 2008 diesel AUTOMATIC                            |0.9620829288975116|
|Mitsubishi Mighty Max Pickup 1994 regular unleaded MANUAL             |0.9484213631039752|
|Chrysler Aspen 2009 regular unleaded AUTOMATIC                        |0.9271092903411216|
|Oldsmobile Cutlass Ciera 1994 regular unleaded AUTOMATIC              |0.911904125634653 |
|Volkswagen Touareg 2015 premium unleaded (recommended) AUTOMATIC      |0.9067141244284443|
|Chrysler TC 1990 regular unleaded MANUAL                              |0.8942706414327495|
|BMW 7 Series 2015 premium unleaded (required) AUTOMATIC               |0.8873266368631819|
|Ferrari Enzo 2003 premium unleaded (required) AUTOMATED_MANUAL        |0.8760739455080375|
|Audi 80 1990 regular unleaded MANUAL                                  |0.871215564244082 |
|Mitsubishi Vanwagon 1990 regular unleaded AUTOMATIC                   |0.8604594927168016|
|Lamborghini Reventon 2008 premium unleaded (required) AUTOMATED_MANUAL|0.8569533527937525|
|Saab 900 1996 regular unleaded MANUAL                                 |0.8517167561268112|
|Ford Focus RS 2017 premium unleaded (recommended) MANUAL              |0.8369213995605174|
|Hyundai Elantra 2017 regular unleaded AUTOMATED_MANUAL                |0.8145671663030841|
|Ford Focus 2017 regular unleaded MANUAL                               |0.8080970764403019|
|Mercedes-Benz E-Class 2015 premium unleaded (required) AUTOMATIC      |0.8045429849258207|
|BMW M4 GTS 2016 premium unleaded (required) AUTOMATED_MANUAL          |0.7994977767050019|
|Chevrolet Cruze 2015 diesel AUTOMATIC                                 |0.7935704861253007|
+----------------------------------------------------------------------+------------------+
only showing top 22 rows
```
