import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.rdd.RDD


// https://stanford.edu/~rezab/classes/cme323/S16/notes/Lecture16/Partitioning_PageRank.pdf
object MatrixMethod {

  def fileToMatrix(file: RDD[String]): RDD[(Int, (Int, Double))] = {
    file.map { line =>
      val edge = line.split("\t")
      (edge(0).toInt, (edge(1).toInt, 1.0))
    }
  }
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
  // You can pass in the uniform vector to save time
  def iterate(pivector: DistrVector, hyperlinks: RDD[(Int, (Int,  Double))], danglers: RDD[(Int, Int)], alpha: Double,
              numNodes: Int, sc: SparkContext, uniform: RDD[Int]): DistrVector = {
    val beginHyperlinkTime = System.nanoTime()
    val hyperLinkPart = pivector.scale(alpha).matrixMult(hyperlinks).addRDD(uniform.map(x => (x, (1.0 - alpha) / numNodes)))
    val hyperLinkTimeTaken = (System.nanoTime() - beginHyperlinkTime) / 1e9d
    debug("Time taken to calculate the hyperlink part " + hyperLinkTimeTaken)
    /*
    // SHOULD BE AGGREGATE, BUT THEN WE NEED TWO FUNCTIONS I AM LAZY AHHH
    val beginDanglerTime = System.nanoTime()
    val pivectorTimesDanglers = pivector.getValues.join(danglers).fold((0, (0,0))) {
      case ((_, (value1, _)), (_, (value2, _))) => (0,(value1 + value2, 0))
    }._2._1
    val endDanglerTime = (System.nanoTime() - beginDanglerTime) / 1e9d
    debug("Time taken to calculate the dangling Node part " + endDanglerTime)
    val danglerPart = uniform.map( index => (index, (alpha * pivectorTimesDanglers + 1 - alpha) / numNodes))
    val result = hyperLinkPart.addRDD(danglerPart)
    */
    hyperLinkPart
  }

  def powerIterations(adjacencyMatrix: RDD[(Int, (Int, Double))], numNodes: Int, sc: SparkContext,
                     numIterations: Int, alpha: Double): DistrVector = {
    val danglers = getDanglers(adjacencyMatrix, numNodes, sc).persist()
    val hyperlinks = toHyperLinkMat(adjacencyMatrix).persist()
    val uniform = sc.parallelize(0 until numNodes).persist()
    var pivector = new DistrVector(uniform.map(x => (x, 1.0 / numNodes)))
    for (i <- 1 to numIterations) {
      debug("Starting iteration " + i)
      val nextPivector = iterate(pivector, hyperlinks, danglers, alpha, numNodes, sc, uniform).cache()
      //println(pivector.infNormDistance(nextPivector))
      pivector = nextPivector
    }
    pivector.scale(numNodes)
  }

  def debug(str: String): Unit = {
    val DEBUG = true
    if (DEBUG) {
      println(str)
    }
  }
  // I think this will be a cleaner way of doing things
  // Adapted from https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPageRank.scala
  // Add in dealing with dangling nodes
  /*
  def powerUntilConvergence(adjacencyList: RDD[(Long, Iterable[Long])], numNodes: Int,
                            sc: SparkContext, tolerance: Double, alpha: Double = 0.15): RDD[(Long, Double)] = {
    val hyperlinks = adjacencyList.mapValues(outLinks => {
      val size = outLinks.size //cache the size
      outLinks.map(id => (id, 1.0 / size))
    })

    //Create danglers
    val possibleNodes = sc.parallelize(0L until numNodes)
    val notDanglers = adjacencyList.map {
      case (node, _) => node
    }
    possibleNodes.subtract(notDanglers)

    val ranks = possibleNodes.map(n => (n, 0, 1, false)) //(index, old pageRank, new pageRank, whether this has terminated)
    val finishedCount = sc.longAccumulator
    while (finishedCount.value != numNodes) {
      val newTraffic = ranks.
    }
  }
  */
}
