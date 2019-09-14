package frl.driesprong.outlierdetection

import breeze.linalg.{DenseVector, sum}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest._

// Unit-tests created based on the Python script of https://github.com/jeroenjanssens/sos
class StocasticOutlierDetectionTest extends FlatSpec with Matchers with BeforeAndAfter with PrivateMethodTester {
  val master = "local"
  val conf: SparkConf = new SparkConf().setAppName(this.getClass().getSimpleName()).setMaster(master)
  val sc = new SparkContext(conf)

  val perplexity = 3.0

  val epsilon = 1e-9f
  implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "Computing the distance matrix " should "give symmetrical distances" in {

    val seqData = Seq(
      (0L, Array(1.0, 3.0)),
      (1L, Array(5.0, 1.0)),
      (2L, Array(2.2, 2.2))
    )

    val data = sc.parallelize(seqData)

    val dMatrix = StochasticOutlierDetection.computeDistanceMatrix(data).collectAsMap()

    dMatrix.size should be(seqData.length)
    // No diagonal
    dMatrix.head._2.length should be(seqData.length - 1)

    dMatrix(0)(0) should be(dMatrix(1)(0))
    dMatrix(0)(1) should be(dMatrix(2)(0))
  }

  "Computing the distance matrix " should "give the correct distances" in {

    val data = sc.parallelize(
      Seq(
        (0L, Array(1.0, 1.0)),
        (1L, Array(2.0, 2.0)),
        (2L, Array(5.0, 1.0))
      ))

    val dMatrix = StochasticOutlierDetection.computeDistanceMatrix(data).collectAsMap()

    dMatrix(0L) should be(Array(Math.sqrt(2.0), Math.sqrt(Math.pow(1.0 - 5.0, 2) + Math.pow(1.0 - 1.0, 2))))
    dMatrix(1L) should be(Array(Math.sqrt(2.0), Math.sqrt(Math.pow(2.0 - 5.0, 2) + Math.pow(2.0 - 1.0, 2))))
    dMatrix(2L) should be(Array(Math.sqrt(16.0), Math.sqrt(10.0)))
  }

  "Computing the perplexity of the vector " should "give the correct error" in {

    val vector = new DenseVector(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 9.0, 10.0))

    val output = Array(
      3.67879441e-01,
      1.35335283e-01,
      4.97870684e-02,
      1.83156389e-02,
      6.73794700e-03,
      2.47875218e-03,
      3.35462628e-04,
      1.23409804e-04,
      4.53999298e-05
    )

    // Standard beta
    val beta = 1.0
    val search = StochasticOutlierDetection.binarySearch(vector, Math.log(perplexity), 500, beta).toArray

    assert(search.length == output.length)
    search.zip(output).foreach(v => assert(v._1 === v._2))
  }

  "Computing the perplexity" should "give the correct perplexity" in {

    val output = StochasticOutlierDetection.getPerplexity(DenseVector(1.0, 2.0, 3.0, 4.0), 3)

    /*
    >>> get_perplexity(np.array([1,2,3,4]), 3)
    (0.2081763951839819, array([4.97870684e-02, 2.47875218e-03, 1.23409804e-04, 6.14421235e-06]))
     */

    output._1 should be(0.2081763951839819)
    output._2 should be(DenseVector(0.049787068367863944, 0.0024787521766663585, 1.2340980408667956E-04, 6.14421235332821E-06))
  }

  "Compute the affinity" should "give the correct affinity" in {

    // The datapoints
    val data = sc.parallelize(
      Seq(
        (0L, Array(1.0, 1.0)),
        (1L, Array(2.0, 1.0)),
        (2L, Array(1.0, 2.0)),
        (3L, Array(2.0, 2.0)),
        (4L, Array(5.0, 8.0)) // The outlier!
      ))

    /*
    Looks ok:
    +---+--------------------------------------------------------------------------------+
    |_1 |_2                                                                              |
    +---+--------------------------------------------------------------------------------+
    |0  |[0.0, 1.0, 1.0, 1.4142135623730951, 8.06225774829855]                           |
    |1  |[1.0, 0.0, 1.4142135623730951, 1.0, 7.615773105863909]                          |
    |2  |[1.0, 1.4142135623730951, 0.0, 1.0, 7.211102550927978]                          |
    |3  |[1.4142135623730951, 1.0, 1.0, 0.0, 6.708203932499369]                          |
    |4  |[8.06225774829855, 7.615773105863909, 7.211102550927978, 6.708203932499369, 0.0]|
    +---+--------------------------------------------------------------------------------+

    df = pd.DataFrame([
      [1.0, 1.0],
      [2.0, 1.0],
      [1.0, 2.0],
      [2.0, 2.0],
      [5.0, 8.0]
    ])

    >>> D = distance.squareform(distance.pdist(df, 'euclidean'))
    >>> D
    array([[0.        , 1.        , 1.        , 1.41421356, 8.06225775],
           [1.        , 0.        , 1.41421356, 1.        , 7.61577311],
           [1.        , 1.41421356, 0.        , 1.        , 7.21110255],
           [1.41421356, 1.        , 1.        , 0.        , 6.70820393],
           [8.06225775, 7.61577311, 7.21110255, 6.70820393, 0.        ]])
     */

    val dMatrix = StochasticOutlierDetection.computeDistanceMatrix(data)

    val dMatrixLocal = dMatrix.collectAsMap()

    dMatrixLocal.size should be(5)
    // No diagonal
    dMatrixLocal.head._2.length should be(4)

    dMatrixLocal(0) should be(Array(1.0, 1.0, 1.4142135623730951, 8.06225774829855))
    dMatrixLocal(1) should be(Array(1.0, 1.4142135623730951, 1.0, 7.615773105863909))
    dMatrixLocal(2) should be(Array(1.0, 1.4142135623730951, 1.0, 7.211102550927978))
    dMatrixLocal(3) should be(Array(1.4142135623730951, 1.0, 1.0, 6.708203932499369))
    dMatrixLocal(4) should be(Array(8.06225774829855, 7.615773105863909, 7.211102550927978, 6.708203932499369))

    val aMatrix = StochasticOutlierDetection.computeAffinityMatrix(
      dMatrix,
      perplexity).collectAsMap()

    /*
    Reference output:
    >>> A = d2a(D)
    >>> A
    array([[0.00000000e+00, 4.64662766e-01, 4.64662766e-01, 3.38268740e-01, 2.07195222e-03],
           [4.48046270e-01, 0.00000000e+00, 3.21289157e-01, 4.48046270e-01, 2.21082346e-03],
           [4.31925257e-01, 3.05063254e-01, 0.00000000e+00, 4.31925257e-01, 2.34905955e-03],
           [2.83704490e-01, 4.10315559e-01, 4.10315559e-01, 0.00000000e+00, 2.53931484e-03],
           [1.65024585e-06, 3.44967767e-06, 6.73004987e-06, 1.54422171e-05, 0.00000000e+00]])
     */

    aMatrix.size should be(5)
    aMatrix.head._2.size should be(4)
    aMatrix(0) should be(DenseVector(0.46466276524577704, 0.46466276524577704, 0.3382687394674377, 0.002071952211368232))
    aMatrix(1) should be(DenseVector(0.44804626736879755, 0.3212891538762665, 0.44804626736879755, 0.0022108233460722557))
    aMatrix(2) should be(DenseVector(0.43192525601205634, 0.30506325262816036, 0.43192525601205634, 0.0023490595181415333))
    aMatrix(3) should be(DenseVector(0.2837044890495805, 0.4103155587026411, 0.4103155587026411, 0.0025393148189994897))
    aMatrix(4) should be(DenseVector(1.6502458086204375E-6, 3.4496775759599478E-6, 6.730049701933432E-6, 1.544221669904019E-5))
  }

  "Verify the binding probabilities " should "give the correct probabilities" in {

    // The distance matrix
    val dMatrix = sc.parallelize(
      Seq(
        (0L, new DenseVector(Array(6.61626106e-112, 1.27343495e-088))),
        (1L, new DenseVector(Array(2.21858114e-020, 1.12846575e-044))),
        (2L, new DenseVector(Array(1.48949023e-010, 1.60381089e-028)))
      ))

    val bMatrix = StochasticOutlierDetection.computeBindingProbabilities(dMatrix).map(_._2).sortBy(dist => sum(dist)).collect()

    assert(bMatrix(0)(0) === 5.19560192e-24)
    assert(bMatrix(0)(1) === 1.00000000e+00)

    assert(bMatrix(1)(0) === 1.00000000e+00)
    assert(bMatrix(1)(1) === 5.08642993e-25)

    assert(bMatrix(2)(0) === 1.00000000e+00)
    assert(bMatrix(2)(1) === 1.07675154e-18)
  }

  "Verifying the product " should "should provide valid products" in {

    val data = sc.parallelize(
      Seq(
        (0L, Array(/*0.0,*/ 0.5, 0.3)),
        (1L, Array(0.25, /*0.0,*/ 0.1)),
        (2L, Array(0.8, 0.8 /*, 0.0*/))
      ))

    val oMatrix = StochasticOutlierDetection.computeOutlierProbability(data).collectAsMap()

    /*
      >>> import pandas as pd
      >>> import numpy as np
      >>>
      >>> df = pd.DataFrame([[0.0, 0.5, 0.3],
      ...                    [0.25, 0.0, 0.1],
      ...                    [0.8, 0.8, 0.0]])
      >>>
      >>> np.prod(1-df, 0)
      0    0.15
      1    0.10
      2    0.63
     */

    val out0 = (1.0 - 0.0) * (1.0 - 0.25) * (1.0 - 0.8) // 0.09999999999999998
    val out1 = (1.0 - 0.5) * (1.0 - 0.0) * (1.0 - 0.8) // 0.14999999999999997
    val out2 = (1.0 - 0.3) * (1.0 - 0.1) * (1.0 - 0) // 0.63

    assert(oMatrix.size == 3)

    assert(oMatrix(0) === out0)
    assert(oMatrix(1) === out1)
    assert(oMatrix(2) === out2)
  }

  "Verifying the output of the SOS algorithm " should "assign the one true outlier" in {

    // The datapoints
    val data = sc.parallelize(
      Seq(
        (0L, Array(1.0, 1.0)),
        (1L, Array(2.0, 1.0)),
        (2L, Array(1.0, 2.0)),
        (3L, Array(2.0, 2.0)),
        (4L, Array(5.0, 8.0)) // The outlier!
      ))

    // Process the steps of the algorithm
    val dMatrix = StochasticOutlierDetection.computeDistanceMatrix(data)

    val aMatrix = StochasticOutlierDetection.computeAffinityMatrix(
      dMatrix,
      perplexity)

    val bMatrix = StochasticOutlierDetection.computeBindingProbabilities(aMatrix)

    val oMatrix = StochasticOutlierDetection.computeOutlierProbability(bMatrix)

    // Do a distributed sort, and then return to driver
    val output = oMatrix.collectAsMap()

    assert(output.size == 5)
    assert(output(0) === 0.27900944792028953)
    assert(output(1) === 0.25775014551682535)
    assert(output(2) === 0.22136130977995763)
    assert(output(3) === 0.12707053787018444)
    assert(output(4) === 0.99227799024537555184) // The outlier!
  }

}
