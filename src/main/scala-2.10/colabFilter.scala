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
import org.apache.spark.mllib.linalg.Vectors


/**
 * Created by sneha on 11/30/2015.
 */
object colabFilter {

  def main(args: Array[String]){

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

    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "hdfs://localhost:19000/track_metadata_without_dup.csv", "header" -> "true"))
    val df_similar = sqlContext.load("com.databricks.spark.csv", Map("path" -> "hdfs://localhost:19000/lastfm_similar_dest.csv", "header" -> "true"))
    df_metadata.registerTempTable("meta_table")
    df_similar.registerTempTable("similar_table")
    //Load user listened music details
    val text_train_triplets = sc.textFile("D:/Project/Dataset/train_triplets_small2.txt")
    val schema_string = "user song_id play_count"
    val schema = StructType( schema_string.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = text_train_triplets.map(_.split("\t")).map(p => Row(p(0), p(1), p(2)))
    val df_train_triplets = sqlContext.createDataFrame(rowRDD,schema)
    df_train_triplets.registerTempTable("triplets_table")

    //Load Attributes
    val df_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/Dataset/song_attribute.csv", "header" -> "true"))
    df_attributes.registerTempTable("attributes")
    //df_train_triplets.show()
    println("User Profile Loaded")
    var user_history_df = profile.get_existing_with_attributes(sqlContext,"b80344d063b5ccb3212f76538f3d9e43d87dca9e" )

   // println("Top 10 songs listened by the user")
    user_history_df.select("play_count","danceability","energy","key","loudness","tempo","time_signature").show(10)

    //Build user profile



  }

}
