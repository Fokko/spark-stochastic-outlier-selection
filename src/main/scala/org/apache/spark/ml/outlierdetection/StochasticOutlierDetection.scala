package org.apache.spark.ml.outlierdetection

import breeze.linalg.functions.euclideanDistance
import breeze.linalg.{DenseVector, sum}
import org.apache.spark.ml.linalg.{DenseVector => SparkDenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

object StochasticOutlierDetection {
  val defaultPerplexity = 30.0
  val defaultEps = 1e-12
  val defaultIterations = 5000

  /**
   * Helper function to make it available to the Python world
   */
  def performOutlierDetectionPython(sqlContext: SQLContext,
                                    ds: Dataset[Row],
                                    labelColumn: String = "label",
                                    vectorColumn: String = "vector",
                                    perplexity: Double = defaultPerplexity,
                                    eps: Double = defaultEps,
                                    maxIterations: Int = defaultIterations): DataFrame = {
    val rdd = performOutlierDetectionDf(ds, labelColumn, vectorColumn, perplexity, eps, maxIterations)

    sqlContext.createDataFrame(rdd).toDF(labelColumn, "score")
  }

  def performOutlierDetectionDs(ds: Dataset[Row],
                                labelColumn: String = "label",
                                vectorColumn: String = "vector",
                                perplexity: Double = defaultPerplexity,
                                eps: Double = defaultEps,
                                maxIterations: Int = defaultIterations): RDD[(String, Double)] = {

    performOutlierDetectionDf(ds, labelColumn, vectorColumn, perplexity, eps, maxIterations)
  }

  def performOutlierDetectionDf(df: DataFrame,
                                labelColumn: String = "label",
                                vectorColumn: String = "vector",
                                perplexity: Double = defaultPerplexity,
                                eps: Double = defaultEps,
                                maxIterations: Int = defaultIterations): RDD[(String, Double)] = {

    val rdd = df.select(labelColumn, vectorColumn).rdd
      .map(row => (row.getAs[String](labelColumn), row.getAs[SparkDenseVector](vectorColumn)))
    performOutlierDetectionRdd(rdd, perplexity, eps, maxIterations)
  }

  def performOutlierDetectionRdd(inputVectors: RDD[(String, SparkDenseVector)],
                                 perplexity: Double = defaultPerplexity,
                                 eps: Double = defaultEps,
                                 maxIterations: Int = defaultIterations): RDD[(String, Double)] = {

    val withIndices = inputVectors.zipWithIndex().map(_.swap)

    val labels = withIndices.mapValues(_._1)
    val vectors = withIndices.mapValues(_._2)

    // Only pass the vectors in
    val dMatrix = computeDistanceMatrix(vectors)
    val aMatrix = computeAffinityMatrix(dMatrix, perplexity, maxIterations, eps)
    val bMatrix = computeBindingProbabilities(aMatrix)
    val oMatrix = computeOutlierProbability(bMatrix)

    oMatrix.join(labels).map(_._2.swap)
  }


  /**
   * Comptues the perplexity for a given vector, given a value of beta
   *
   * @param D    The input vector
   * @param beta The given beta
   * @return A tuple of the current error and the affinity vector
   */
  private[outlierdetection] def getPerplexity(D: DenseVector[Double], beta: Double): (Double, DenseVector[Double]) = {
    val A = D.map(a => Math.exp(-a * beta))
    val sumA = sum(A)
    val h = Math.log(sumA) + beta * sum(A :* D) / sumA
    (h, A)
  }

  /**
   * Computes the affinity for a given row of the distance matrix
   *
   * @param D             The input vector of the given row
   * @param logPerplexity The log taken from the perplexity
   * @param maxIterations The maximum number of iterations before giving up
   * @param eps           The accepted error
   * @param iteration     The current iteration
   * @param beta          The current approximated beta
   * @param betaMin       The lower bound of the beta
   * @param betaMax       The upper bound of the beta
   * @return
   */
  @scala.annotation.tailrec
  private[outlierdetection] def binarySearch(D: DenseVector[Double],
                                             logPerplexity: Double,
                                             maxIterations: Int,
                                             eps: Double,
                                             iteration: Int = 0,
                                             beta: Double = 1.0,
                                             betaMin: Double = Double.NegativeInfinity,
                                             betaMax: Double = Double.PositiveInfinity): DenseVector[Double] = {

    val (h, matA) = getPerplexity(D, beta)
    val hDiff = h - logPerplexity

    if (iteration < maxIterations && Math.abs(hDiff) > eps) {
      val (newBeta, newBetaMin, newBetaMax) = if (hDiff.isNaN) {
        // If the beta is too high, it might result into a NaN
        (beta / 10.0, betaMin, betaMax)
      } else {
        if (hDiff > 0)
          (if (betaMax == Double.PositiveInfinity || betaMax == Double.NegativeInfinity)
            beta * 2.0
          else
            (beta + betaMax) / 2.0, beta, betaMax)
        else
          (if (betaMin == Double.PositiveInfinity || betaMin == Double.NegativeInfinity)
            beta / 2.0
          else
            (beta + betaMin) / 2.0, betaMin, beta)
      }

      binarySearch(D, logPerplexity, maxIterations, eps, iteration + 1, newBeta, newBetaMin, newBetaMax)
    }
    else
      matA
  }

  /**
   * Will convert the distances into the affinity by sampling using a binary search.
   * The binary search will stop if it is between a certain tolerance
   *
   * @param dMatrix       The distance matrix
   * @param perplexity    The perplexity
   * @param maxIterations The maximum number of iterations before stopping
   * @param eps           The accepted error to stop early
   * @return The affinity matrix
   */
  private[outlierdetection] def computeAffinityMatrix(dMatrix: RDD[(Long, Array[Double])],
                                                      perplexity: Double = defaultPerplexity,
                                                      maxIterations: Int = defaultIterations,
                                                      eps: Double = defaultEps): RDD[(Long, DenseVector[Double])] =
    dMatrix.mapValues(r => binarySearch(new DenseVector(r), Math.log(perplexity), maxIterations, eps))

  /**
   * Scales the binding probabilities by dividing the values in the vector by the total sum
   *
   * @param rows The affinity values
   * @return Scaled affinity values where the sum is equal to 1.0
   */
  private[outlierdetection] def computeBindingProbabilities(rows: RDD[(Long, DenseVector[Double])]): RDD[(Long, Array[Double])] =
    rows.mapValues(row => (row :/ sum(row)).toArray)

  /**
   * Accepts a RDD of (Index, Vector) which is being used to compute the distance from each vector to each other vector.
   * We're removing the diagonal since it isn't of any importance. Mostly because for computing the affection we don't
   * want to include the distance to the vector itself.
   *
   * @param data RDD of (Index, Vector) as input
   * @return RDD of (Index, Vector) where the position in the vector is the Index of the other vector
   */
  private[outlierdetection] def computeDistanceMatrix(data: RDD[(Long, SparkDenseVector)]): RDD[(Long, Array[Double])] =
    data
      .cache()
      .cartesian(data)
      .map(row => row._1._1 -> (row._2._1, euclideanDistance(row._1._2.asBreeze, row._2._2.asBreeze)))
      // Remove the distance to itself, i.e. the diagonal of the matrix
      .filter(row => row._1 != row._2._1)
      .groupByKey()
      .mapValues(_.toArray.sortBy(_._1).map(_._2))

  /**
   * This will multiply all the columns with each other so we get the
   * final outlier probability in [0, 1]
   *
   * @param rows The rows with the affinity
   * @return The outlier probabilities for each of the observations
   */
  def computeOutlierProbability(rows: RDD[(Long, Array[Double])]): RDD[(Long, Double)] =
    rows
      .flatMap(row =>
        row._2.zipWithIndex.map(value => {
          val beyondDiagonal = if (value._2 >= row._1) 1L else 0L
          (value._2 + beyondDiagonal, value._1)
        })
      )
      .groupByKey()
      .mapValues(vector => vector.fold(1.0)((a, b) => a * (1.0 - b)))

}
