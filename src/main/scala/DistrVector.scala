import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.rdd.RDD

class DistrVector(values: RDD[(Int, Double)]) {

  def getValues: RDD[(Int, Double)] = values

  def scale(scalar: Double): DistrVector = {
    new DistrVector(values.map { case (key, value) => (key, scalar * value) })
  }

  //https://stackoverflow.com/questions/37225266/how-to-use-spark-to-implement-matrix-vector-multiplication
  // assuming matrices of the form (row, (col, value)) and the vector is on the left, so that indices of the vector
  // match to rows
  def matrixMult(mat: RDD[(Int, (Int,  Double))]): DistrVector = {
    val withScalars = values.join(mat)
    // We just care about the column and vector value * matrix value
    val newValues = withScalars map {
      case (row, (vecValue , (col, matValue))) => (col, (row, vecValue * matValue))
    } map {
      case (col, (_, value)) => (col, value)
    } reduceByKey {
      case (x, y) => x + y
    }

    new DistrVector(newValues)
  }

  def addRDD(rdd: RDD[(Int, Double)]): DistrVector = {
    new DistrVector(values.leftOuterJoin(rdd).map {
      case (index, (vectorVal, otherVal)) => (index, vectorVal + otherVal.getOrElse(0.0))
    })
  }

  override def toString: String = "DistrVector(" + values.toString() + ")"

  //For debugging only, bad performance in parallel or something maybe
  def printAll(): Unit = {
    values.foreach(x => println(x))
  }
}
