import org.apache.spark.SparkConf
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.avro
import scala.collection.immutable.SortedMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.Row

import scala.tools.cmd.FromString.StringFromString

object engine {
  def main(args: Array[String])
  {
    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir","c:/winutil/")

    val conf = new SparkConf().setAppName("MusicReco").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    //df_userdata > dataframe for user data
    val text_train_triplets = sc.textFile("hdfs://localhost:19000/train_triplets_small.txt")
    val schema_string = "user song_id play_count"
    val schema = StructType( schema_string.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = text_train_triplets.map(_.split("\t")).map(p => Row(p(0), p(1), p(2)))
    val df_train_triplets = sqlContext.createDataFrame(rowRDD,schema)

    //df_metadata > dataframe for songs metadata
    //df_similar >  dataframe for similar songs
    //loading the dataframe from HDFC ; currently local node

    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "hdfs://localhost:19000/track_metadata_without_dup.csv", "header" -> "true"))
    val df_similar = sqlContext.load("com.databricks.spark.csv", Map("path" -> "hdfs://localhost:19000/lastfm_similar_dest.csv", "header" -> "true"))

    df_metadata.registerTempTable("meta_table")
    df_similar.registerTempTable("similar_table")
    df_train_triplets.registerTempTable("triplets_table")

    var user_history_df = profile.get_existing(sqlContext,"b80344d063b5ccb3212f76538f3d9e43d87dca9e" )
    println("User Profile Loaded")
    println("Top 10 songs listened by the user")
    user_history_df.select("title","artist_name","release","duration","year","play_count").show(10)

    var user_history_list =  user_history_df.select("track_id","play_count").limit(10).map(r => Row(r(0),r(1)))

    //converting RDD to List
    var list_of_songs = Map[String, Int]()
    //var row =Array(String)
    for (temp <- user_history_list.collect() )
    {
      var row = temp.toString().split(",")
      list_of_songs +=  (row(0).drop(1).toString -> row(1).dropRight(1).toInt)

    }

    println("Recomended Songs for the user")

    // song.getDetails(sqlContext, song.get_similar(sqlContext,list_of_songs).keysIterator ).show(50) //coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/newcars.csv")
    song.FinalResult(sc,sqlContext, song.get_similar(sqlContext,list_of_songs)).select("title","artist_name","release","duration","year","reco_conf").show(50)

    sc.stop()
    println("Spark Context stopped")
  }

}
