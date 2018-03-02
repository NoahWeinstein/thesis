
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object BasicPageRank {
  def main(args: Array[String]): Unit = {
    val isAws = false
    val conf = new SparkConf().setAppName("BasicPageRank").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    if (isAws) {

      val fileName = "s3://thesisgraphs/small-graph.txt"

      val graph = GraphLoader.edgeListFile(sc, fileName)

      //https://spark.apache.org/docs/latest/graphx-programming-guide.html#pagerank
      val ranks = graph.pageRank(0.0001).vertices

      ranks.coalesce(1).saveAsTextFile("s3://thesisgraphs/output")
    } else {

      val fileName = "web-Google.txt"
      val file = sc.textFile(fileName)

      //vectorTest(sc)
      matrixMethodTest(sc)
      graphMethodTest(sc)

      //Matrix Method First
      val adjacencyMatrix = MatrixMethod.fileToMatrix(file).partitionBy(new HashPartitioner(8)).persist()
      println(adjacencyMatrix.getNumPartitions)

      //HARD CODED LMAO
      val numNodes = 875713
      println(numNodes)
      val startTime = System.nanoTime()
      val powerIterationResult = MatrixMethod.powerIterations(adjacencyMatrix, numNodes, sc, 10, 0.85)
      println(powerIterationResult.getValues.count())
      val timeTaken = (System.nanoTime() - startTime) / 1e9d
      powerIterationResult.getValues.saveAsTextFile("powerOutput")
      println(timeTaken)
      adjacencyMatrix.unpersist()

      //Built in Graph Method

      val webGraph = GraphLoader.edgeListFile(sc, fileName)
      val graphStartTime = System.nanoTime()
      val rankedGraph = webGraph.staticPageRank(50).vertices  //(0.001).vertices
      val graphTimeTaken = (System.nanoTime() - graphStartTime) / 1e9d
      println(graphTimeTaken)
      val graphRanks = new DistrVector(rankedGraph.map {
        case (id, value) => (id.toInt, value)
      })
      println("SOME ERRORS")
      println(powerIterationResult.infNormDistance(graphRanks))
      println(powerIterationResult.euclidDistance(graphRanks))
      //rankedGraph.saveAsTextFile("graphOutput")

      //https://stackoverflow.com/questions/37730808/how-i-know-the-runtime-of-a-code-in-scala
      //matrixMethodTest(sc)
    }
//458 2733
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

  def matrixMethodTest(sc: SparkContext): Unit = {
    val sparseMatrix = sc.parallelize(Seq(
      (0, (0, 2.0)), (0, (1, 3.0)),
      (1, (0, 1.0)), (1, (1, 2.0)),
      (2, (0, 0.0)), (2, (1, 0.0))
    ))
    // MatrixMethod.getDanglers(sparseMatrix, 6, sc).foreach(x => println(x))
    val numNodes = 4
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
    //val hyperlinks = MatrixMethod.toHyperLinkMat(origExampleAdj)
    //val danglers = MatrixMethod.getDanglers(origExampleAdj, numNodes, sc)
    //val uniform = new DistrVector(sc.parallelize(Seq((0, 0.25), (1, 0.25), (2, 0.25), (3, 0.25))))
    //MatrixMethod.iterate(uniform, hyperlinks, danglers, 0.85, 4, sc).printAll()
    val startTime = System.nanoTime()
    val powerIterationResult = MatrixMethod.powerIterations(withDanglers, numNodes, sc, 50, 0.85)
    val timeTaken = (System.nanoTime() - startTime) / 1e9d
    println(timeTaken)
    println("PRINTING POWER METHOD WITH ONE DANGLER")
    powerIterationResult.printAll()
    println("DONE WITH POWER METHOD")

    /*
    val joinTester1 = sc.parallelize(Seq((0, 1), (1, 2)))
    val joinTester2 = sc.parallelize(Seq((1, 3)))
    joinTester1.join(joinTester2).foreach(x => println(x))
    */
  }

  def graphMethodTest(sc: SparkContext): Unit = {
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

    doMCTest(danglerGraph, 80)
  }

  def doMCTest(webGraph: Graph[Int, Int], numIters: Int): Unit = {
    val mcStartTime = System.nanoTime()
    val mcPageRanks = MonteCarloMethod.doMNWalks(webGraph, numIters)
    val mcTimeTaken = (System.nanoTime() - mcStartTime) / 1e9d
    println("PRINTING MC TEST WITH DANGLER")
    mcPageRanks.foreach(println)
    println("DONE WITH MC TEST")
  }

}
