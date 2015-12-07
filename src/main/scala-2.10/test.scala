import java.util.Calendar

import breeze.numerics.abs
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.evaluation.{RegressionMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{IntegerType, StructType, StructField, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.joda.time
import org.joda.time.{Interval, DateTime}
import org.joda.time.base.AbstractInterval
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object dropDup {
  def main(args: Array[String]) {
    //remove logging from console

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")

    var startTime = new DateTime()
    println("start",startTime)
    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    println(abs(-3))
    val df_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes1.csv", "header" -> "true"))
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))

    println(abs(-3))
    val arr = new Array[String](1)
    arr(0) = "track_id"
    val arr1 = new Array[String](1)
    //arr1(0) = "track_id1"
    //println(df_metadata.dropDuplicates(arr).count())
    //println(df_attributes.dropDuplicates(arr1).count())
  //  val df_final = df_attributes.join(df_metadata, df_metadata("track_id") === df_attributes("track_id1")).select("track_id",	"danceability",	"energy",	"key",	"loudness",	"tempo",	"time_signature").dropDuplicates(arr)
   // println(df_final.count())
   val endTime = new DateTime()
    val per = new time.Interval(startTime,endTime)

    println(endTime)

    println("Time to train:" , per.toDuration.getStandardSeconds, "seconds")

      //select("track_id",	"danceability",	"energy",	"key",	"loudness",	"tempo",	"time_signature").toDF("track_id",	"danceability",	"energy",	"key",	"loudness",	"tempo",	"time_signature")
    //df_final.coalesce(8).write.format("com.databricks.spark.csv").option("header","true").save("D:/Project/FinalDataset/home6.csv")

  }
}

object test {
  def main(args: Array[String]) {
    //remove logging from console

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")

    var startTime = new DateTime()
    println("start",startTime)
    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

   // val data = sc.textFile("D:/Project/spark/data/mllib/sample_isotonic_regression_data.txt")

    // Create label, feature, weight tuples from input data with weight set to default value 1.0.


    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/Dataset/test.csv", "header" -> "true"))
    val similar_songs_RDD_LP = df_metadata.map(r=>LabeledPoint(r(0).toString.toDouble,Vectors.dense(r(1).toString.toDouble,r(2).toString.toDouble,r(3).toString.toDouble)))
   // val test : Vector = Vectors.dense(2.0,3.0,4.0)
    // Split data into training (60%) and test (40%) sets.


    // Create isotonic regression model from training data.
    // Isotonic parameter defaults to true so it is only shown for demonstration
    //neALS.
    similar_songs_RDD_LP.foreach(println)
    //val model = LassoWithSGD.train(similar_songs_RDD_LP, 12)

   /* println(model.predict(Vectors.dense(220,330,440)))
    println(model.weights)*/
    // Create tuples of predicted and real labels.
    /*val predictionAndLabel = test.map { point =>
      val predictedLabel = model.predict(point._2)
      (predictedLabel, point._1)*/



    // Calculate mean squared error between predicted and real labels.
   /* val meanSquaredError = predictionAndLabel.map{case(p, l) => math.pow((p - l), 2)}.mean()
    println("Mean Squared Error = " + meanSquaredError)

    //val predictionAndLabels : RDD[(Double,Double)] = predictionAndLabel.toDF().map(l => (l(1).toString.toDouble,l(2).toString.toDouble))
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val precision = metrics.precision
    println("Precision = " + precision)
    println("End: Prediction")*/
  }
}


object test2{
  def main(args: Array[String]) {
    //remove logging from console

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")

    var startTime = new DateTime()
    println("start",startTime)
    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    val data = MLUtils.loadLibSVMFile(sc, "D:/Project/spark/data/mllib/sample_linear_regression_data.txt").cache()
    data.foreach(println)
    // Build the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(data, numIterations)

    // Get predictions
    val valuesAndPreds = data.map{ point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }
    valuesAndPreds.foreach(println)

    // Instantiate metrics object
    val metrics = new RegressionMetrics(valuesAndPreds)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")


  }


}
