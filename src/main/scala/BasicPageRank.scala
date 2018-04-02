
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.io.{BufferedWriter, File, FileWriter}

/*
Main procedure where I run experiments from
 */
object BasicPageRank {
  def main(args: Array[String]): Unit = {
    val isAws = true
    val conf = if (isAws) new SparkConf().setAppName("BasicPageRank")
                else new SparkConf().setAppName("BasicPageRank").setMaster("local[2]")
    val sc = new SparkContext(conf)
    if (!isAws) {
      sc.setCheckpointDir("checkpoints")
    }
    sc.setLogLevel("WARN")
    val fileName = if (isAws) "s3://thesisgraphs/web-Google.txt" else "web-Google.txt"

    val file = sc.textFile(fileName)
    val experiment = "4machines10partsFIX"
    val matrixOutput = if (isAws) s"s3://thesisgraphs/$experiment/matrixOutput" else "matrixOutput50"
    val graphOutput = if (isAws) s"s3://thesisgraphs/$experiment/graphOutput" else "graphOutput50"
    val errorOutput = if (isAws) s"s3://thesisgraphs/$experiment/errors" else "errors10partfix"
    val timesOutput = if (isAws) s"s3://thesisgraphs/$experiment/times" else "times10partfix"

    val numIters = 10

    // val smallPowerPageRanks = matrixMethodTest(sc)
    // graphMethodTest(sc, Some(smallPowerPageRanks))

    //Matrix Method First
    val adjacencyMatrix = MatrixMethod.fileToMatrix(file).partitionBy(new HashPartitioner(8)).persist()

    val startTime = System.nanoTime()
    val powerIterationResult = MatrixMethod.powerIterations(adjacencyMatrix, sc, numIters, 0.85)
    val timeTaken = (System.nanoTime() - startTime) / 1e9d
    //powerIterationResult.getValues.coalesce(1).saveAsTextFile(matrixOutput)
    adjacencyMatrix.unpersist()

    //Built in Graph Method

    val webGraph = GraphLoader.edgeListFile(sc, fileName, numEdgePartitions = 8)
    val graphStartTime = System.nanoTime()
    val rankedGraph = webGraph.staticPageRank(numIters).vertices  //(0.001).vertices
    val graphTimeTaken = (System.nanoTime() - graphStartTime) / 1e9d
    val graphRanks = new DistrVector(rankedGraph.map {
      case (id, value) => (id.toInt, value)
    })

    //rankedGraph.coalesce(1).saveAsTextFile(graphOutput)

    //Monte Carlo Method
//    val mcPageRanks = new DistrVector(MonteCarloMethod.doMNWalks(webGraph, numIters).map({
//      case (key, value) => (key.toInt, value)
//    }))

    val infError = powerIterationResult.infNormDistance(graphRanks)
    val euclideanError = powerIterationResult.euclidDistance(graphRanks)
//    val mcInfError = powerIterationResult.infNormDistance(mcPageRanks)
//    val mcEuclidError = powerIterationResult.euclidDistance(mcPageRanks)
    val infRelativeToMatrix = infError / powerIterationResult.infNorm()
    val eucildRelativeToMatrix = euclideanError / powerIterationResult.euclidNorm()
    val infRelativeToGraph = infError / graphRanks.infNorm()
    val euclidRelativeToGraph = euclideanError / graphRanks.euclidNorm()
//    val mcInfRelativeToMatrix = mcInfError / powerIterationResult.infNorm()
//    val mcEucildRelativeToMatrix = mcEuclidError / powerIterationResult.euclidNorm()

    val times = sc.parallelize(Seq(
      ("Power Iteration", timeTaken),
      ("Built-in", graphTimeTaken)
    ), 1)
    times.saveAsTextFile(timesOutput)
    val errors = sc.parallelize(Seq(
      ("Infinity Error", infError),
      ("Euclidean Error", euclideanError),
      ("Infinity Error Relative to Matrix", infRelativeToMatrix),
      ("Euclidean Error Relative to Matrix", eucildRelativeToMatrix),
      ("Infinity Error Relative to Graph", infRelativeToGraph),
      ("Euclidean Error Relative to Graph", euclidRelativeToGraph)
//      ("MC Infinity Error", mcInfError),
//      ("MC Euclidean Error", mcEuclidError),
//      ("MC Infinity Error Relative to Matrix", mcInfRelativeToMatrix),
//      ("MC Euclidean Error Relative to Matrix", mcEucildRelativeToMatrix)
    ), 1)
    errors.saveAsTextFile(errorOutput)
    //https://stackoverflow.com/questions/37730808/how-i-know-the-runtime-of-a-code-in-scala
    //matrixMethodTest(sc)
  }

  def vectorTest(sc: SparkContext): Unit = {
    val testVector = new DistrVector(sc.parallelize(Seq((0, 5.0))))
    val testMatrix = sc.parallelize(Seq((0,(0, 4.0))))
    val multiplied = testVector.matrixMult(testMatrix)
    //multiplied.getValues.foreach(x => println(x))

    val biggerVector = new DistrVector(sc.parallelize(Seq((0, 5.0), (1, 3.0), (2, 2.0))))
    val otherVector = new DistrVector(sc.parallelize(Seq((0, 4.0), (1, 1.0), (2, 4.0))))
    val fatMatrix = sc.parallelize(Seq(
      (0, (0, 2.0)), (0, (1, 3.0)),
      (1, (0, 1.0)), (1, (1, 2.0)),
      (2, (0, 0.0)), (2, (1, 0.0))
    ))
    val bigMultiplied = biggerVector.matrixMult(fatMatrix)
    bigMultiplied.getValues.foreach(x => println(x))
    println(biggerVector.euclidDistance(otherVector))
  }

  def matrixMethodTest(sc: SparkContext): DistrVector = {
    val sparseMatrix = sc.parallelize(Seq(
      (0, (0, 2.0)), (0, (1, 3.0)),
      (1, (0, 1.0)), (1, (1, 2.0)),
      (2, (0, 0.0)), (2, (1, 0.0))
    ))
    // MatrixMethod.getDanglers(sparseMatrix, 6, sc).foreach(x => println(x))
    val origExampleAdj = sc.parallelize(Seq(
      (0, (1, 1.0)), (0, (2, 1.0)), (0, (3, 1.0)),
      (1, (2, 1.0)),
      (2, (0, 1.0)),
      (3, (0, 1.0)), (3, (2, 1.0))
    ))
    val withDanglers = sc.parallelize(Seq(
      (0, (1, 1.0)), (0, (2, 1.0)), (0, (3, 1.0)),
      (1, (2, 1.0)),
      (3, (0, 1.0)), (3, (2, 1.0))
    ))
    val twoDanglers = sc.parallelize(Seq(
      (0, (1, 1.0)), (0, (2, 1.0)), (0, (3, 1.0)),
      (1, (2, 1.0)), (1, (4, 1.0)),
      (3, (0, 1.0)), (3, (2, 1.0))
    ))
    //val hyperlinks = MatrixMethod.toHyperLinkMat(origExampleAdj)
    //val danglers = MatrixMethod.getDanglers(origExampleAdj, numNodes, sc)
    //val uniform = new DistrVector(sc.parallelize(Seq((0, 0.25), (1, 0.25), (2, 0.25), (3, 0.25))))
    //MatrixMethod.iterate(uniform, hyperlinks, danglers, 0.85, 4, sc).printAll()
    val startTime = System.nanoTime()
    val powerIterationResult = MatrixMethod.powerIterations(withDanglers, sc, 50, 0.85)
    val timeTaken = (System.nanoTime() - startTime) / 1e9d
    val twoDanglerResult = MatrixMethod.powerIterations(twoDanglers, sc, 50, 0.85)
    println(timeTaken)
    println("PRINTING POWER METHOD WITH ONE DANGLER")
    powerIterationResult.printAll()
    println("DONE WITH POWER METHOD")
    println("PRINTING POWER METHOD WITH TWO DANGLERS")
    twoDanglerResult.printAll()
    println("DONE WITH TWO DANGLER POWER METHOD")

    /*
    val joinTester1 = sc.parallelize(Seq((0, 1), (1, 2)))
    val joinTester2 = sc.parallelize(Seq((1, 3)))
    joinTester1.join(joinTester2).foreach(x => println(x))
    */
    powerIterationResult
  }

  def graphMethodTest(sc: SparkContext, errorAgainst: Option[DistrVector] = None): Unit = {
    val vertices: RDD[(VertexId, Int)] = sc.parallelize(Array((0L, 1), (1L, 1), (2L, 1), (3L, 1)))
    val edges = sc.parallelize(Array(
      Edge(0L, 1L, 1), Edge(0L, 2L, 1), Edge(0L, 3L, 1),
      Edge(1L, 2L, 1),
      Edge(3L, 0L, 1), Edge(3L, 2L, 1)
    ))
    val danglerGraph = Graph(vertices, edges)
    val startTime = System.nanoTime()
    val pageRanks = danglerGraph.staticPageRank(50).vertices
    val timeTaken = (System.nanoTime() - startTime) / 1e9d
    println("PRINTING GRAPH TEST WITH DANGLER")
    pageRanks.foreach(println)
    println("DONE WITH GRAPH TEST")
    val twoDanglerVers: RDD[(VertexId, Int)] = sc.parallelize(Array((0L, 1), (1L, 1), (2L, 1), (3L, 1), (4L, 1)))
    val twoDanglerEdge = sc.parallelize(Array(
      Edge(0L, 1L, 1), Edge(0L, 2L, 1), Edge(0L, 3L, 1),
      Edge(1L, 2L, 1), Edge(1L, 4L, 1),
      Edge(3L, 0L, 1), Edge(3L, 2L, 1)
    ))
    val twoDanglerGraph = Graph(twoDanglerVers, twoDanglerEdge)
    val twoDanglerRanks = twoDanglerGraph.staticPageRank(50).vertices
    println("PRINTING GRAPH TEST WITH TWO DANGLERS")
    twoDanglerRanks.foreach(println)
    println("DONE WITH SECOND GRAPH TEST")
    doMCTest(danglerGraph, 50, errorAgainst)
  }

  def doMCTest(webGraph: Graph[Int, Int], numIters: Int, errorAgainst: Option[DistrVector]): Unit = {
    val mcStartTime = System.nanoTime()
    val mcPageRanks = MonteCarloMethod.doMNWalks(webGraph, numIters)
    val mcTimeTaken = (System.nanoTime() - mcStartTime) / 1e9d
    println("PRINTING MC TEST WITH DANGLER")
    mcPageRanks.foreach(println)
    println("DONE WITH MC TEST")
    val mcVector = new DistrVector(mcPageRanks.map({
      case (key, value) => (key.toInt, value)
    }))
    errorAgainst.foreach(
      (expected) => {
        val euclidError = mcVector.euclidDistance(expected)
        println("MC ERRORS")
        println(euclidError)
        println(euclidError / expected.euclidNorm())
      }
    )
  }

}
