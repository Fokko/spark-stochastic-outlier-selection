package frl.driesprong.outlierdetection

import breeze.linalg.{DenseVector, sum}
import breeze.numerics.{pow, sqrt}
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

object StochasticOutlierDetection {

  def performOutlierDetection(inputVectors: RDD[Array[Double]],
                              perplexity: Double = 30.0,
                              eps: Double = 1e-20,
                              maxIterations: Int = 5000): collection.Map[Long, Double] = {

    val iMatrix = inputVectors.zipWithIndex().map(_.swap)

    val dMatrix = computeDistanceMatrix(iMatrix)
    val aMatrix = computeAffinityMatrix(dMatrix, perplexity, maxIterations, eps)
    val bMatrix = computeBindingProbabilities(aMatrix)
    val oMatrix = computeOutlierProbability(bMatrix)

    oMatrix.collectAsMap()
  }

  private[outlierdetection] def getPerplexity(D: DenseVector[Double], beta: Double): (Double, DenseVector[Double]) = {
    val A = D.map(a => Math.exp(-a * beta))
    val sumA = sum(A)
    val h = Math.log(sumA) + beta * sum(A :* D) / sumA
    (h, A)
  }

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
      val (newBeta, newBetaMin, newBetaMax) = if (hDiff > 0)
        (if (betaMax == Double.PositiveInfinity || betaMax == Double.NegativeInfinity)
          beta * 2.0
        else
          (beta + betaMax) / 2.0, beta, betaMax)
      else
        (if (betaMin == Double.PositiveInfinity || betaMin == Double.NegativeInfinity)
          beta / 2.0
        else
          (beta + betaMin) / 2.0, betaMin, beta)

      binarySearch(D, logPerplexity, maxIterations, eps, iteration + 1, newBeta, newBetaMin, newBetaMax)
    }
    else
      matA
  }

  private[outlierdetection] def computeAffinityMatrix(dMatrix: RDD[(Long, Array[Double])],
                                                      perplexity: Double,
                                                      maxIterations: Int = 5000,
                                                      eps: Double = 1e-20): RDD[(Long, DenseVector[Double])] =
    dMatrix.mapValues(r => binarySearch(new DenseVector(r), Math.log(perplexity), maxIterations, eps))

  private[outlierdetection] def euclDistance(a: Array[Double], b: Array[Double]): Double = sqrt((a zip b).map { case (x, y) => pow(y - x, 2) }.sum)

  private[outlierdetection] def computeBindingProbabilities(rows: RDD[(Long, DenseVector[Double])]): RDD[(Long, Array[Double])] =
    rows.map(r => (r._1, (r._2 :/ sum(r._2)).toArray))

  /**
   * Accepts a RDD of (Index, Vector) which is being used to compute the distance from each vector to each other vector.
   * We're removing the diagonal since it isn't of any importance. Mostly because for computing the affection we don't
   * want to include the distance to the vector itself.
   *
   * @param data RDD of (Index, Vector) as input
   * @return RDD of (Index, Vector) where the position in the vector is the Index of the other vector
   */
  private[outlierdetection] def computeDistanceMatrix(data: RDD[(Long, Array[Double])]): RDD[(Long, Array[Double])] =
    data
      .cartesian(data)
      .map(row => row._1._1 -> (row._2._1, euclDistance(row._1._2, row._2._2)))
      // Remove the distance to itself
      .filter(row => row._1 != row._2._1)
      .groupByKey()
      .mapValues(_.toArray.sortBy(_._1).map(_._2))

  /**
   * This will multiply all the columns
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
      .mapValues(_.fold(1.0)((a, b) => a * (1.0 - b)))

}
