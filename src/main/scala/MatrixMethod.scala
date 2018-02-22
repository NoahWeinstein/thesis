import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MatrixMethod {

  def getDanglers(adjacencyMatrix: RDD[(Int, (Int, Double))], numNodes: Int, sc: SparkContext): RDD[(Int, Int)] = {
    val possibleNodes = sc.parallelize(0 until numNodes)
    val notDanglers = adjacencyMatrix.map {
      case (node, _) => node
    }
    possibleNodes.subtract(notDanglers).map(x => (x, 1))
  }

  // Takes an adjacency matrix and turns it into a hyperlink matrix.
  def toHyperLinkMat(adjacencyMatrix: RDD[(Int, (Int, Double))]): RDD[(Int, (Int, Double))] = {
    val linkCounts = adjacencyMatrix.aggregateByKey(0)((accum: Int, x: (Int, Double)) => accum + 1,
      (accum1: Int, accum2: Int) => accum1 + accum2)
    adjacencyMatrix
      .join(linkCounts)
      .map {
        case (row, ((col, _), numLinks)) => (row, (col, 1.0 / numLinks))
      }
  }

  // Idea: pik+1 = alpha * pik * hyperlinks + (alpha *  pik * danglers + 1 - alpha) * uniform / n
  def iterate(pivector: DistrVector, hyperlinks: RDD[(Int, (Int,  Double))], danglers: RDD[(Int, Int)], alpha: Double,
              numNodes: Int, sc: SparkContext): DistrVector = {
    val hyperLinkPart = pivector.scale(alpha).matrixMult(hyperlinks)
    val uniform = sc.parallelize(0 until numNodes)
    // SHOULD BE AGGREGATE, BUT THEN WE NEED TO FUNCTIONS I AM LAZY AHHH
    val pivectorTimesDanglers = pivector.getValues.join(danglers).fold((0, (0,0))) {
      case ((_, (value1, _)), (_, (value2, _))) => (0,(value1 + value2, 0))
    }._2._1
    val danglerPart = uniform.map( index => (index, (alpha * pivectorTimesDanglers + 1 - alpha) / numNodes))
    hyperLinkPart.addRDD(danglerPart)
  }

  def powerIterations(adjacencyMatrix: RDD[(Int, (Int, Double))], numNodes: Int, sc: SparkContext,
                     numIterations: Int, alpha: Double): DistrVector = {
    val danglers = getDanglers(adjacencyMatrix, numNodes, sc)
    val hyperlinks = toHyperLinkMat(adjacencyMatrix)
    var pivector = new DistrVector(sc.parallelize(0 until numNodes).map(x => (x, 1.0 / numNodes)))
    for (i <- 1 to numIterations) {
      pivector = iterate(pivector, hyperlinks, danglers, alpha, numNodes, sc)
    }
    pivector
  }
}
