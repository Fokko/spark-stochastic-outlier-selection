import math
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, lit, min, max, asc, desc, concat_ws
from pyspark.mllib.common import _java2py

df = spark.read.option("header", "true").csv("data/cardataset.csv")

metric_columns = ["Engine HP", "Engine Cylinders", "highway MPG", "city mpg", "Popularity", "MSRP"]

# Scale the columns and remove the empty ones
for column_name in metric_columns:
    df = df.withColumn(column_name, col(column_name).cast("Double"))
    minValue = lit(df.select(min(col(column_name)).alias("min")).first().asDict()['min'])
    maxValue = lit(df.select(max(col(column_name)).alias("max")).first().asDict()['max'])
    print("Col " + column_name + " min " + str(minValue) + ", max: " + str(maxValue))
    df = df.where(col(column_name).isNotNull())
    df = df.withColumn(column_name, (col(column_name) - minValue) / (maxValue - minValue))


df = df.withColumn("label", concat_ws(" ", col("Make"), col("Model"), col("Year"), col("Engine Fuel Type"), col("Transmission Type")))

# Count the number of rows
num = df.count()
num

# Remove the missing vectors and combine all the columns to a single vector
ass = VectorAssembler(inputCols=metric_columns, outputCol="vector")
df = ass.setHandleInvalid("skip").transform(df).repartition(22)

# As perplexity, use the sqrt of the number of rows
perplexity = math.sqrt(num)

jvm = sc._jvm
sqlContext = df._jdf


sos = jvm.org.apache.spark.ml.outlierdetection.StochasticOutlierDetection().performOutlierDetectionPython(spark._jwrapped, df._jdf, "label", "vector", perplexity)

# Reconstruct the Python DF
result_df = _java2py(sc, sos)

result_df.orderBy(asc("score")).show(22, False)

result_df.orderBy(desc("score")).show(22, False)
