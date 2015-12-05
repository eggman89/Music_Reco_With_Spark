import java.util.Calendar

import breeze.numerics.abs
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
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