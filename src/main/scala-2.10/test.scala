import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import java.util.Random
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering.{PowerIterationClustering, PowerIterationClusteringModel}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
object test {


  def main(args: Array[String]) {
    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    //System.setProperty("hadoop.home.dir","c:/winutil/")

    val conf = new SparkConf().setAppName("MusicReco").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

   /* val data = sc.textFile("D:/Project/spark/data/mllib/kmeans_data.txt")

    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    parsedData.foreach(println)
    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    print("tes")
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    println(clusters)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    //build matrix
    val denseData = Seq(
      Vectors.dense(0.0, 1.0, 2.0),
      Vectors.dense(3.0, 4.0, 5.0),
      Vectors.dense(6.0, 7.0, 8.0),
      Vectors.dense(9.0, 0.0, 1.0)
    )

    println(denseData)

    val dm: Matrix = Matrices.dense(7, 6, Array(10,10,10,10,10,10,10,10,5,5,5,5,5,5,10,5,5,5,5,5,5,5,10,5,5,5,5,5,5,10,5,5,5,5,5,5,10,15,15,15,15,15))
    println(dm)
    //val rows: RDD[Vector] = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0)) // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    //val rows: RDD[Vector] = denseData

    // Get its size.
    //val m = mat.numRows()
    //val n = mat.numCols()

    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    val rows = matrixToRDD(sc, dm)
    print("this")
    val mat = new RowMatrix(rows)

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(4, computeU = true)
    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V

    //println(U)
    //println(s)
   // println(V)*/

   // println(mat.computePrincipalComponents(1))
    //val projected: RowMatrix = mat.multiply(mat.computePrincipalComponents(1))
    //println(projected)

    val freqItemsets = sc.parallelize(Seq(
      new FreqItemset(Array("a"), 15L),
      new FreqItemset(Array("b"), 35L),
      new FreqItemset(Array("c"), 70L),
      new FreqItemset(Array("a", "b"), 12L),
      new FreqItemset(Array("b", "c"), 24L),
      new FreqItemset(Array("a", "c"), 12L)
    ));

    val ar = new AssociationRules()
      .setMinConfidence(0.8)
    val results = ar.run(freqItemsets)

    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "=>"
        + rule.consequent.mkString(",") + "]," + rule.confidence)
    }
  }

  def matrixToRDD(sc: SparkContext , m: Matrix): RDD[Vector] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }

}
