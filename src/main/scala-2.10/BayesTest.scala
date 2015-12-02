import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
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
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
object BayesTest {

  def main(args: Array[String]) {

    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")

    val conf = new SparkConf().setAppName("MusicReco").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)


    //val data = sc.textFile("D:/Project/spark/data/mllib/sample_naive_bayes_data2.txt")
    val data = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/spark/data/mllib/bayes.csv", "header" -> "true"))
    val parsedData = data.map{ line => LabeledPoint(line(0).toString.toDouble, Vectors.dense(line(1).toString.toDouble,line(2).toString.toDouble, line(3).toString.toDouble, line(4).toString.toDouble))}
   // val parsedData = data.map { line =>
   //   LabeledPoint(line(0).toDouble, Vectors.dense(line(1).toDouble,line(2).toDouble, line(3).toDouble, line(4).toDouble))
    //}

    //creating vector
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0, 4.0)
    println(dv)
    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = parsedData
    println("Training")
    training.foreach(println)
    //val test = splits(1)
    println("Test")
    //test.foreach(println)

    val model = NaiveBayes.train(training, lambda = 3.0, modelType = "multinomial")

   // val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    //val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()


    val dm: Matrix = Matrices.dense(1, 4, Array(0,0,0,0))
    println("dm")
    println(dm)
    val rows = matrixToRDD(sc, dm)
    println("predict")
    model.predict(rows).foreach(println)
    model.predictProbabilities(rows).foreach(println)
    //model.save(sc, "D:/Project/spark/data/mllib/")
    //val sameModel = NaiveBayesModel.load(sc, "D:/Project/spark/data/mllib/")
    //println(accuracy)
  }
    def matrixToRDD(sc: SparkContext , m: Matrix): RDD[Vector] = {
      val columns = m.toArray.grouped(m.numRows)
      val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
      val vectors = rows.map(row => new DenseVector(row.toArray))
      sc.parallelize(vectors)
    }



}
