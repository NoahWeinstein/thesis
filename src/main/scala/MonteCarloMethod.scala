import org.apache.spark.graphx.{EdgeDirection, Graph}
import scala.util.Random

//Idea, map each vertex in a graph to a random edge... need a list of out nodes for each vertex, is that possible?

object MonteCarloMethod {

  def doNWalks (webGraph: Graph[Int, Int], numIters: Int): Unit = {
    val numNodes = webGraph.vertices.count()
    val neighbors = webGraph.collectNeighborIds(EdgeDirection.Out)
    var walksGraph = webGraph.mapVertices(
      (_, _) => (1, 1, Array[Long]()) // Number of current visitors, total number of visitors
    ).joinVertices(neighbors)({
      case (_, _, neighbs) => (1, 1, neighbs)
    })
    for (i <- 1 to numIters) {
      val nextVisits = walksGraph.vertices.flatMap({
        case (id, (numSurfers, _, neighbs)) => {
          // Randomly select a next node to visit
          if (!neighbs.isEmpty) {
            for (j <- 1 to numSurfers) yield (neighbs(Random.nextInt(neighbs.length)), 1)
          } else {
            Array[(Long, Int)]()
          }
        }
      }).reduceByKey(_ + _) //Collect all visits to each node
      // Reset and add visits
      walksGraph = walksGraph.joinVertices(nextVisits)({
        case (_, (_, totalVisits, neighbs), newVisits) =>
          (newVisits, totalVisits + newVisits, neighbs)
      })
    }
  }
}
