//Possible this is not necessary, LMAO

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import scala.util.hashing.MurmurHash3
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId

object GraphParser {
  def fileToGraph(fileName: String)(implicit sc: SparkContext) : Graph[Int, Int] = {

    // Mostly taken from https://stackoverflow.com/questions/32396477/how-to-create-a-graph-from-a-csv-file-using-graph-fromedgetuples-in-spark-scala
    val file = sc.textFile(fileName)
    // create edge RDD of type RDD[(VertexId, VertexId)]
    val edgesRDD: RDD[(VertexId, VertexId)] = file.map(line => line.split("\t"))
      .map(line => (MurmurHash3.stringHash(line(0)), MurmurHash3.stringHash(line(1))))

    // create a graph
    Graph.fromEdgeTuples(edgesRDD, 1)
  }
}
