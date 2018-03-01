import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.rdd.RDD

class DistrVector(values: RDD[(Int, Double)]) {

  def getValues: RDD[(Int, Double)] = values

  def scale(scalar: Double): DistrVector = {
    new DistrVector(values.mapValues { value => scalar * value })
  }

  //https://stackoverflow.com/questions/37225266/how-to-use-spark-to-implement-matrix-vector-multiplication
  // assuming matrices of the form (row, (col, value)) and the vector is on the left, so that indices of the vector
  // match to rows
  def matrixMult(mat: RDD[(Int, (Int,  Double))]): DistrVector = {
    val withScalars = values.join(mat)
    // We just care about the column and vector value * matrix value
    val newValues = withScalars map {
      case (_, (vecValue , (col, matValue))) => (col, vecValue * matValue)
    } reduceByKey {
      case (x, y) => x + y
    }

    new DistrVector(newValues)
  }

  def euclidDistance(other: DistrVector): Double = {
    // Find differemces squared, then reduce
    Math.sqrt(values.join(other.getValues).map {
      case (_, (x1, x2)) => Math.pow(x1 - x2, 2)
    } reduce {
      (a, b) => a + b
    })
  }

  def infNormDistance(other: DistrVector): Double = {
    values.join(other.getValues).map {
      case (_, (x1, x2)) => Math.abs(x1 - x2)
    } reduce {
      (a, b) => Math.max(a, b)
    }
  }

  def addRDD(rdd: RDD[(Int, Double)]): DistrVector = {
    new DistrVector(values.leftOuterJoin(rdd).mapValues {
      case (vectorVal, otherVal) => vectorVal + otherVal.getOrElse(0.0)
    })
  }

  def cache(): DistrVector = {
    new DistrVector(values.cache())
  }

  override def toString: String = "DistrVector(" + values.toString() + ")"

  //For debugging only, bad performance in parallel or something maybe
  def printAll(): Unit = {
    values.foreach(x => println(x))
  }
}
