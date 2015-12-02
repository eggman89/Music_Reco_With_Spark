import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.avro
import scala.collection.immutable.SortedMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{IntegerType, StructType, StructField, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD


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
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    //df_userdata > dataframe for user data
    //val text_train_triplets = sc.textFile("hdfs://localhost:19000/train_triplets_small.txt")
    val text_train_triplets = sc.textFile("D:/Project/Dataset/train_triplets_small2.txt")
    val schema_string = "user song_id play_count"

    val schema = StructType( schema_string.split(" ").map(fieldName =>
    if(fieldName == "play_count" ) StructField(fieldName, IntegerType, true)
    else StructField(fieldName, StringType, true))
    )

    val rowRDD = text_train_triplets.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).toInt))
    val df_train_triplets = sqlContext.createDataFrame(rowRDD,schema)

    //df_metadata > dataframe for songs metadata
    //df_similar >  dataframe for similar songs
    //loading the dataframe from HDFS ; currently local node

    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "hdfs://localhost:19000/track_metadata_without_dup.csv", "header" -> "true"))
    val df_similar = sqlContext.load("com.databricks.spark.csv", Map("path" -> "hdfs://localhost:19000/lastfm_similar_dest.csv", "header" -> "true"))
    val df_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/Dataset/song_attribute.csv", "header" -> "true"))


    df_metadata.registerTempTable("meta_table")
    df_similar.registerTempTable("similar_table")
    df_attributes.registerTempTable("attributes")
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
    //song.get_similar(sqlContext,list_of_songs).keysIterator.foreach(println)
    //song.getAttributes(sqlContext,list_of_songs.keysIterator).
    // song.getDetails(sqlContext, song.get_similar(sqlContext,list_of_songs).keysIterator ).show(50) //coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("D:/newcars.csv")
    var similar_songs = song.get_similar(sqlContext,list_of_songs)
    var similar_songs_RDD : RDD[(String, Float)] = sc.parallelize(similar_songs.toSeq)
    var similar_songs_DF = similar_songs_RDD.toDF()
    val songs_attr = song.getAttributes(sqlContext,similar_songs.keysIterator)


    songs_attr.printSchema()
    similar_songs_DF.printSchema()

    var similar_songs_attr = songs_attr.join(similar_songs_DF,songs_attr("track_id") === similar_songs_DF("_1")).select("track_id","danceability","energy","loudness","tempo","_2")

    similar_songs_attr.orderBy(similar_songs_attr("_2").desc).show(10)
    print("end")
    song.FinalResult(sc,sqlContext, song.get_similar(sqlContext,list_of_songs)).select("title","artist_name","release","duration","year","reco_conf").show(50)

    sc.stop()

    println("Spark Context stopped")
  }

}
