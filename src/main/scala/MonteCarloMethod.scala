import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD

import scala.util.Random

//Idea, map each vertex in a graph to a random edge... need a list of out nodes for each vertex, is that possible?

object MonteCarloMethod {
  val r = new Random()
  def doMNWalks (webGraph: Graph[Int, Int], numIters: Int, alpha: Double = 0.85): RDD[(Long, Double)] = {
    val numNodes = webGraph.vertices.count()
    val neighbors = webGraph.collectNeighborIds(EdgeDirection.Out)
    var walksGraph = webGraph.mapVertices(
      (_, _) => (1, 1, Array[Long]()) // Number of current visitors, total number of visitors
    ).joinVertices(neighbors)({
      case (_, _, neighbs) => (1, 1, neighbs)
    })
    for (i <- 1 to numIters) {
      //println("Iteration " + i)
      var nextVisits = getRandomVisits(walksGraph, alpha) //Collect all visits to each node
      // Reset and add visits
      while (!nextVisits.isEmpty()) { // Don't terminate until all walks have visited dangling nodes
        //println(nextVisits.count())
        walksGraph = walksGraph.outerJoinVertices(nextVisits)({
          case (_, (_, totalVisits, neighbs), maybeVisits) => {
            val newVisits = maybeVisits.getOrElse(0) //Default to 0 new vistors
            //println("New visitors " + newVisits)
            //println("Before updating " + totalVisits)
            (newVisits, totalVisits + newVisits, neighbs) // All the current visitors have left, add the new visitors to the total
          }
        })
        nextVisits = getRandomVisits(walksGraph, alpha)
        walksGraph = walksGraph.mapVertices({ //Reset current visitors to 1
          case (id, (_, totalVisits, neighbs)) => (1, totalVisits, neighbs)
        })
      }
    }
    val combinedVisits = walksGraph.vertices.map({
      case (_, (_, visits, _)) => visits
    }).reduce(_ + _)
    //println(combinedVisits)
    walksGraph.vertices.map({
      case (id, (_, visits, _)) => (id, visits.toDouble / combinedVisits)
    })
  }


  def getRandomVisits(walksGraph: Graph[(Int, Int, Array[Long]), Int], alpha: Double): RDD[(Long, Int)] = {
    walksGraph.vertices.flatMap({
      case (id, (numSurfers, _, neighbs)) => {
        // Randomly select a next node to visit
        if (!neighbs.isEmpty) {
          for (j <- 1 to numSurfers; if r.nextDouble() < alpha) yield (neighbs(r.nextInt(neighbs.length)), 1)
        } else {
          Array[(Long, Int)]()
        }
      }
    }).reduceByKey(_ + _)
  }
}

