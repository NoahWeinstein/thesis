
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
      vectorTest(sc)
      matrixMethodTest(sc)
    }

  }

  def vectorTest(sc: SparkContext): Unit = {
    val testVector = new DistrVector(sc.parallelize(Seq((0, 5.0))))
    val testMatrix = sc.parallelize(Seq((0,(0, 4.0))))
    val multiplied = testVector.matrixMult(testMatrix)
    //multiplied.getValues.foreach(x => println(x))

    val biggerVector = new DistrVector(sc.parallelize(Seq((0, 5.0), (1, 3.0), (2, 2.0))))
    val fatMatrix = sc.parallelize(Seq(
      (0, (0, 2.0)), (0, (1, 3.0)),
      (1, (0, 1.0)), (1, (1, 2.0)),
      (2, (0, 0.0)), (2, (1, 0.0))
    ))
    val bigMultiplied = biggerVector.matrixMult(fatMatrix)
    bigMultiplied.getValues.foreach(x => println(x))
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
    //val hyperlinks = MatrixMethod.toHyperLinkMat(origExampleAdj)
    //val danglers = MatrixMethod.getDanglers(origExampleAdj, numNodes, sc)
    //val uniform = new DistrVector(sc.parallelize(Seq((0, 0.25), (1, 0.25), (2, 0.25), (3, 0.25))))
    //MatrixMethod.iterate(uniform, hyperlinks, danglers, 0.85, 4, sc).printAll()
    MatrixMethod.powerIterations(origExampleAdj, numNodes, sc, 40, 0.85).printAll()

    /*
    val joinTester1 = sc.parallelize(Seq((0, 1), (1, 2)))
    val joinTester2 = sc.parallelize(Seq((1, 3)))
    joinTester1.join(joinTester2).foreach(x => println(x))
    */
  }

}
